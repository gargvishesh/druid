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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.imply.druid.loading.SegmentLifecycleLogger;
import io.imply.druid.loading.SegmentReplacementStrategy;
import io.imply.druid.loading.VirtualSegmentMetadata;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * For the purpose of this class, refer to docs of {@link VirtualSegmentStateManager}.
 */
public class VirtualSegmentStateManagerImpl implements VirtualSegmentStateManager
{
  private final SegmentReplacementStrategy strategy;
  public static final Logger LOG = new Logger(VirtualSegmentStateManagerImpl.class);
  private final ConcurrentHashMap<SegmentId, VirtualSegmentMetadata> segmentMetadataMap;

  @Inject
  public VirtualSegmentStateManagerImpl(
      final SegmentReplacementStrategy segmentReplacementStrategy
  )
  {
    this.strategy = segmentReplacementStrategy;
    this.segmentMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public VirtualReferenceCountingSegment registerIfAbsent(VirtualReferenceCountingSegment segment)
  {
    SegmentLifecycleLogger.LOG.debug("Trying to add the segment [%s]", segment.getId());
    VirtualSegmentMetadata previous = segmentMetadataMap.putIfAbsent(
        segment.getId(),
        new VirtualSegmentMetadata(segment, Status.READY)
    );
    if (null == previous) {
      SegmentLifecycleLogger.LOG.debug("Added a new segment [%s]", segment.getId());
      return null;
    }
    return previous.getVirtualSegment();
  }

  @Override
  public ListenableFuture<Void> queue(VirtualReferenceCountingSegment segment)
  {
    if (null == segment) {
      throw new IAE("Null segment being queued");
    }

    VirtualSegmentMetadata newMetadata = segmentMetadataMap.compute(segment.getId(), (key, metadata) -> {
      SegmentLifecycleLogger.LOG.debug("Queuing data segment [%s]", segment.getId());
      if (null == metadata) {
        throw new ISE("segment [%s] must be added first before queuing", segment.getId());
      }
      switch (metadata.getStatus()) {
        case READY:
          metadata.setStatus(Status.QUEUED);
          metadata.setDownloadFuture(SettableFuture.create());
          strategy.queue(segment.getId(), metadata);
          SegmentLifecycleLogger.LOG.debug(
              "Transitioned [%s] from [%s] to [%s]",
              segment.getId(),
              Status.READY,
              Status.QUEUED
          );
          break;
        case DOWNLOADED:
          Preconditions.checkArgument(
              metadata.getDownloadFuture().isDone(),
              "Segment [%s] is downloaded but future is incomplete",
              segment.getId()
          );
          SegmentLifecycleLogger.LOG.debug("[%s] is already downloaded", segment.getId());
          break;
        case ERROR:
          //TODO: what exception
          Preconditions.checkArgument(
              metadata.getDownloadFuture().isDone(),
              "Segment [%s] is errored but future is incomplete",
              segment.getId()
          );
          SegmentLifecycleLogger.LOG.debug("[%s] is in error state", segment.getId());
          break;
        case QUEUED:
          Preconditions.checkArgument(
              !metadata.getDownloadFuture().isDone(),
              "Segment [%s] is queued but future is already complete",
              segment.getId()
          );
          //TODO: fix this
          // It will be better to introduce a new status called `DOWNLOADING` and queue the segment only if the state
          // transitions from DOWNLOADING to QUEUED.
          strategy.queue(segment.getId(), metadata);
          SegmentLifecycleLogger.LOG.debug("[%s] is already queued", segment.getId());
          break;
        default:
          throw new ISE("unknown state: %s", metadata.getStatus());
      }
      return metadata;
    });

    // Instead of returning newMetadata.getDownloadFuture() itself, we return a different Future object. This is done
    // because we do not want the download future to be set or cancelled outside this class without the protection of
    // concurrency guards present in this class.
    SettableFuture<Void> resultFuture = SettableFuture.create();
    Futures.addCallback(newMetadata.getDownloadFuture(), new FutureCallback<Void>()
    {
      @Override
      public void onSuccess(@Nullable Void result)
      {
        resultFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t)
      {
        if (t instanceof CancellationException) {
          resultFuture.cancel(true);
          return;
        }
        resultFuture.setException(t);
      }
    });
    return resultFuture;
  }

  @Override
  @Nullable
  //TODO: add contract explicitly stating that two successive toProcess can't return same segment
  public synchronized VirtualReferenceCountingSegment toDownload()
  {
    //TODO: promote to downloading
    SegmentId segmentId = strategy.nextProcess();
    if (segmentId == null) {
      return null;
    }
    SegmentLifecycleLogger.LOG.debug("[%s] returned to be downloaded next", segmentId);
    VirtualSegmentMetadata metadata = segmentMetadataMap.get(segmentId);
    if (metadata.getVirtualSegment() == null) {
      throw new ISE("DataSegment [%s] has no virtual segment associated", segmentId);
    }
    return metadata.getVirtualSegment();
  }

  @Override
  public void downloaded(VirtualReferenceCountingSegment segment)
  {
    segmentMetadataMap.compute(segment.getId(), (key, metadata) -> {
      SegmentLifecycleLogger.LOG.debug("Marking [%s] as downloaded", segment.getId());
      if (null == metadata) {
        throw new ISE("segment [%s] must be added first before download", segment.getId());
      }
      if (metadata.getStatus() == Status.DOWNLOADED) {
        SegmentLifecycleLogger.LOG.debug("[%s] is already marked downloaded", segment.getId());
        return metadata;
      }
      if (segment.isEvicted()) {
        SegmentLifecycleLogger.LOG.debug("Unevicting the segment [%s]", segment.getId());
        segment.unEvict();
      }
      strategy.downloaded(segment.getId(), metadata);
      if (null == metadata.getDownloadFuture()) {
        Preconditions.checkArgument(
            metadata.getStatus() == Status.READY,
            "Expected status: [%s], Found: [%s]",
            Status.READY,
            metadata.getStatus()
        );
        metadata.setDownloadFuture(SettableFuture.create());
      }

      metadata.getDownloadFuture().set(null);
      metadata.setStatus(Status.DOWNLOADED);
      return metadata;
    });
  }

  @Override
  public void downloadFailed(VirtualReferenceCountingSegment segment, Throwable th, boolean recoverable)
  {
    //TODO
  }

  @Override
  public void evict(VirtualReferenceCountingSegment segment)
  {
    segmentMetadataMap.compute(segment.getId(), (key, metadata) -> {
      SegmentLifecycleLogger.LOG.debug("Evicting segment [%s]", segment.getId());
      if (null == metadata || metadata.getStatus() != Status.DOWNLOADED) {
        throw new ISE(
            "[%s] being asked to evict but is not marked downloaded. Current state [%s]",
            segment.getId(),
            (null == metadata) ? null : metadata.getStatus()
        );
        // todo: alert
      }
      //Following could throw SegmentNotEvictableException
      segment.evict();
      strategy.remove(segment.getId());
      metadata.resetFuture();
      metadata.setStatus(Status.READY);
      return metadata;
    });
  }

  @Override
  public void remove(SegmentId segmentId)
  {
    segmentMetadataMap.compute(segmentId, (key, metadata) -> {
      if (null == metadata) {
        throw new ISE("Being asked to remove a segment [%s] that is not added yet", segmentId);
      }
      VirtualReferenceCountingSegment segmentReference = metadata.getVirtualSegment();
      if (null != segmentReference) {
        try {
          segmentReference.close();
        }
        catch (Exception ex) {
          LOG.error(ex, "Failed to close the segment [%s]", segmentId);
        }
      }
      strategy.remove(segmentId);
      if (metadata.getStatus() == Status.QUEUED) {
        // fail the future with an exception
        metadata.getDownloadFuture().setException(new ISE("[%s] Segment was removed", segmentId));
      }
      return null;
    });
  }

  @Override
  public VirtualReferenceCountingSegment get(SegmentId segmentId)
  {
    VirtualSegmentMetadata metadata = segmentMetadataMap.get(segmentId);
    if (null == metadata) {
      return null;
    }
    return metadata.getVirtualSegment();
  }

  @Override
  //TODO: change status?
  public synchronized VirtualReferenceCountingSegment toEvict()
  {
    SegmentId segmentId = strategy.nextEvict();
    if (segmentId == null) {
      return null;
    }
    SegmentLifecycleLogger.LOG.debug("[%s] to be evicted next", segmentId);
    VirtualSegmentMetadata metadata = segmentMetadataMap.get(segmentId);
    if (metadata == null) {
      throw new ISE("No record of segment [%s]", segmentId);
    }
    if (metadata.getVirtualSegment() == null) {
      throw new ISE("DataSegment [%s] has no virtual segment associated", segmentId);
    }
    return metadata.getVirtualSegment();
  }

  @VisibleForTesting
  int size()
  {
    return segmentMetadataMap.size();
  }

  @VisibleForTesting
  VirtualSegmentMetadata getMetadata(SegmentId segmentId)
  {
    return segmentMetadataMap.get(segmentId);
  }

  public enum Status
  {
    READY,
    QUEUED,
    DOWNLOADED,
    ERROR
  }
}
