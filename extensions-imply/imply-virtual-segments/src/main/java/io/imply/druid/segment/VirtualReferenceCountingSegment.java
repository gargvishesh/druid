/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.loading.SegmentLifecycleLogger;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * VirtualReferenceCountingSegment is used to track references of virtual segments. It differs from regular
 * ReferenceCountingSegment in that
 * - once ReferenceCountingSegment is closed, it cannot be opened again. VirtualReferenceCountingSegment can be
 * evicted - unevicted multiple times.
 * - VirtualReferenceCountingSegment has additional methods to know if the segment is in use by any query. This is
 * helpful in knowing if the segment can be deleted from disk safely and unmapped from memory.
 * - ReferenceCountingSegment can be closed while it is in use. The base object is closed once the user releases the
 * reference. VirtualReferenceCountingSegment is evicted only when it is not in use.
 * <p>
 * Reference maintenance has to be synchronized to support above features. When the eviction starts, query system
 * should not be able to get the reference.
 */
public class VirtualReferenceCountingSegment extends ReferenceCountingSegment
{
  private static final Logger LOG = new Logger(VirtualReferenceCountingSegment.class);
  private final AtomicBoolean evicted = new AtomicBoolean(false);
  private final AtomicInteger references = new AtomicInteger(0);

  protected VirtualReferenceCountingSegment(
      VirtualSegment baseSegment,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion,
      short atomicUpdateGroupSize
  )
  {
    super(baseSegment, startRootPartitionId, endRootPartitionId, minorVersion, atomicUpdateGroupSize);
  }

  public static VirtualReferenceCountingSegment wrapRootGenerationSegment(VirtualSegment baseSegment)
  {
    return new VirtualReferenceCountingSegment(
        Preconditions.checkNotNull(baseSegment, "baseSegment"),
        baseSegment.getId().getPartitionNum(),
        (baseSegment.getId().getPartitionNum() + 1),
        (short) 0,
        (short) 1
    );
  }

  public static VirtualReferenceCountingSegment wrapSegment(
      VirtualSegment baseSegment,
      ShardSpec shardSpec
  )
  {
    return new VirtualReferenceCountingSegment(
        baseSegment,
        shardSpec.getStartRootPartitionId(),
        shardSpec.getEndRootPartitionId(),
        shardSpec.getMinorVersion(),
        shardSpec.getAtomicUpdateGroupSize()
    );
  }

  @Nullable
  @Override
  public Segment getBaseSegment()
  {
    return baseObject;
  }

  @Override
  @Nullable
  public SegmentId getId()
  {
    // Virtual segment always return the id even if real segment is closed
    return baseObject.getId();
  }

  @Override
  @Nullable
  public Interval getDataInterval()
  {
    // Virtual segment always returns the data interval even if real segment is closed
    return baseObject.getDataInterval();
  }

  @Override
  @Nullable
  public QueryableIndex asQueryableIndex()
  {
    return !isClosed() ? baseObject.asQueryableIndex() : null;
  }

  @Override
  @Nullable
  public StorageAdapter asStorageAdapter()
  {
    return !isClosed() ? baseObject.asStorageAdapter() : null;
  }

  @VisibleForTesting
  @Nullable
  public Segment getRealSegment()
  {
    return isClosed() || isEvicted() ? null : ((VirtualSegment) baseObject).getRealSegment();
  }

  @Override
  public boolean increment()
  {
    synchronized (references) {
      references.incrementAndGet();
      return super.increment();
    }
  }

  @Override
  public void decrement()
  {
    synchronized (references) {
      references.decrementAndGet();
      super.decrement();
    }
  }

  public void evict() throws SegmentNotEvictableException
  {
    synchronized (references) {
      if (references.get() != 0) {
        LOG.error("Reference count should have been zero. Instead [%d] for segment [%s]", references.get(), getId());
        throw new SegmentNotEvictableException(this.getId());
      }
      if (evicted.compareAndSet(false, true)) {
        SegmentLifecycleLogger.LOG.debug("Evicting segment [%s]", this.getId());
        try {
          SegmentLifecycleLogger.LOG.debug("Closing base object [%s] since its now evictable", getId());
          baseObject.close();
        }
        catch (Exception e) {
          LOG.error(e, "Exception while closing reference counted object[%s]", getId());
        }
      } else {
        LOG.warn("evict() is called more than once on VirtualReferenceCountingSegment %s", getId());
      }
    }
  }

  public void unEvict()
  {
    if (evicted.compareAndSet(true, false)) {
      SegmentLifecycleLogger.LOG.debug("Unevicting segment [%s]", this.getId());
    } else {
      LOG.warn("unEvict() is called more than once on VirtualReferenceCountingSegment: [%s]", this.getId());
    }
  }

  @Override
  public void close()
  {
    SegmentLifecycleLogger.LOG.debug("Closing virtual segment [%s]", this.getId());
    evicted.set(false);
    super.close();
  }

  public boolean isEvicted()
  {
    return evicted.get();
  }

  public boolean isEvictable()
  {
    synchronized (references) {
      return references.get() <= 0;
    }
  }

  public boolean hasActiveQueries()
  {
    synchronized (references) {
      return references.get() > 0;
    }
  }
}
