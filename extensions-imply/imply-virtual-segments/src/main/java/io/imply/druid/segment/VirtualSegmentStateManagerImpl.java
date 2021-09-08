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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


/**
 * For the purpose of this class, refer to docs of {@link VirtualSegmentStateManager}.
 *
 * <p>
 * Valid state transitions (NULL refers to no state). Each list item is an origin state and nested list items are
 * destination state. There are scenarios in comments to reflect those state transitions.
 *
 * NULL
 *    - READY (new segment has been assigned)
 * READY
 *    - QUEUED (segment is queued for download)
 *    - DOWNLOADED (segment was already cached)
 *    - NULL (segment was un-assigned)
 * QUEUED
 *    - QUEUED  (different request comes for the same segment)
 *    - DOWNLOADED (segment that was queued for download, is now downloaded)
 *    - READY (segment is most likely cancelled)
 *    - NULL (segment was un-assigned while it was queued. This can happen if queries are cancelled but segment is removed
 *    entirely before the segment download is cancelled)
 * DOWNLOADED
 *    - QUEUED (state transition is ignored without any error)
 *    - DOWNLOADED (unlikely to happen but transition is ignored without any error)
 *    - READY (segment was evicted due to lack of disk space but is still assigned to this server)
 *    - NULL (segment was un-assigned and removed from historical)
 *
 * </p>
 */

public class VirtualSegmentStateManagerImpl implements VirtualSegmentStateManager
{
  private final SegmentReplacementStrategy strategy;
  public static final Logger LOG = new Logger(VirtualSegmentStateManagerImpl.class);
  private final ConcurrentHashMap<SegmentId, VirtualSegmentMetadata> segmentMetadataMap;
  private final VirtualSegmentStats virtualSegmentsStats;

  @Inject
  public VirtualSegmentStateManagerImpl(
      final SegmentReplacementStrategy segmentReplacementStrategy,
      final VirtualSegmentStats virtualSegmentStats
  )
  {
    this.strategy = segmentReplacementStrategy;
    this.virtualSegmentsStats = virtualSegmentStats;
    this.segmentMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public VirtualReferenceCountingSegment registerIfAbsent(VirtualReferenceCountingSegment segment)
  {
    SegmentLifecycleLogger.LOG.debug("Trying to add the segment [%s]", segment.getId());
    final AtomicReference<VirtualReferenceCountingSegment> previous = new AtomicReference<>(null);
    segmentMetadataMap.compute(segment.getId(), (key, metadata) -> {
          if (null == metadata) {
            return new VirtualSegmentMetadata(segment, Status.READY);
          }
          if (metadata.isToBeRemoved()) {
            LOG.info("Unmarking a to be removed segment [%s]", segment.getId());
            metadata.unmarkToBeRemoved();
          }
          previous.set(metadata.getVirtualSegment());
          return metadata;
        }
    );
    if (null == previous.get()) {
      SegmentLifecycleLogger.LOG.debug("Added a new segment [%s]", segment.getId());
      return null;
    }
    return previous.get();
  }

  @Override
  public void requeue(VirtualReferenceCountingSegment segment)
  {
    queueInternal(segment, null, true);
  }

  @Override
  public ListenableFuture<Void> queue(VirtualReferenceCountingSegment segment, Closer closer)
  {
    VirtualSegmentMetadata newMetadata = queueInternal(segment, closer, false);
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

  private VirtualSegmentMetadata queueInternal(VirtualReferenceCountingSegment segment, @Nullable Closer closer, boolean isRequeue)
  {
    if (null == segment) {
      throw new IAE("Null segment being queued");
    }

    return segmentMetadataMap.compute(segment.getId(), (key, metadata) -> {
      SegmentLifecycleLogger.LOG.debug("Queuing data segment [%s]", segment.getId());
      if (null == metadata) {
        throw new ISE("segment [%s] must be added first before queuing", segment.getId());
      }
      if (metadata.isToBeRemoved() && !isRequeue) {
        throw new IAE("[%s] is being queried while it is already unassigned from server. " +
            "Consider increasing 'druid.segmentCache.dropSegmentDelayMillis'", segment.getId());
      }

      switch (metadata.getStatus()) {
        case READY:
          metadata.setStatus(Status.QUEUED);
          metadata.setQueueStartTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
          virtualSegmentsStats.incrementQueued();
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
        case QUEUED:
          Preconditions.checkArgument(
              !metadata.getDownloadFuture().isDone(),
              "Segment [%s] is queued but future is already complete",
              segment.getId()
          );
          //TODO: fix this. Below need to be called only during re-queuing.
          // It will be better to introduce a new status called `DOWNLOADING` and queue the segment only if the state
          // transitions from DOWNLOADING to QUEUED.
          strategy.queue(segment.getId(), metadata);
          SegmentLifecycleLogger.LOG.debug("[%s] is already queued", segment.getId());
          break;
        default:
          throw new ISE("unknown state: %s", metadata.getStatus());
      }

      if (null != closer && !isRequeue) {
        // Make sure the reference is acquired and cleaned when closer is closed.
        closer.register(
            metadata
                .getVirtualSegment()
                .acquireReferences()
                .orElseThrow(() -> new ISE("could not acquire the reference for segment [%s]", segment.getId())));
      }
      return metadata;
    });
  }

  @Override
  public boolean cancelDownload(VirtualReferenceCountingSegment segment)
  {
    if (null == segment) {
      throw new IAE("Download of null segment being cancelled");
    }

    VirtualSegmentMetadata computedMetadata = segmentMetadataMap.compute(segment.getId(), (key, metadata) -> {
      SegmentLifecycleLogger.LOG.debug("Cancelling download of data segment [%s]", segment.getId());
      if (null == metadata) {
        throw new ISE("Segment [%s] must be added first before cancelling download", segment.getId());
      }
      switch (metadata.getStatus()) {
        case QUEUED:
          // Verify the sanity of the state
          Preconditions.checkArgument(
              !metadata.getDownloadFuture().isDone(),
              "Segment [%s] is queued but future is already complete",
              segment.getId()
          );

          // Cancel the download if the segment has no active query
          if (!metadata.getVirtualSegment().hasActiveQueries()) {
            strategy.remove(segment.getId());
            metadata.setStatus(Status.READY);
            metadata.resetFuture();
            SegmentLifecycleLogger.LOG.debug(
                "Transitioned [%s] from [%s] to [%s]",
                segment.getId(),
                Status.QUEUED,
                Status.READY
            );
          }
          break;
        case READY:
        case DOWNLOADED:
          SegmentLifecycleLogger.LOG.debug(
              "Doing nothing since state is already [%s]",
              metadata.getStatus()
          );
          break;
        default:
          throw new ISE("unknown segment state: %s for segment [%s]", metadata.getStatus(), segment.getId());
      }
      return metadata;
    });

    return computedMetadata.getStatus() != Status.QUEUED;
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
      if (metadata.getStatus() == Status.QUEUED) {
        final long queueStartTime = metadata.getQueueStartTimeMillis();
        if (queueStartTime != 0) {
          virtualSegmentsStats.recordDownloadWaitingTime(
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
              - queueStartTime);
        }
      }

      metadata.getDownloadFuture().set(null);
      metadata.setStatus(Status.DOWNLOADED);
      virtualSegmentsStats.incrementDownloaded();
      return metadata;
    });
  }

  @Override
  public void downloadFailed(VirtualReferenceCountingSegment segment, Throwable th, boolean recoverable)
  {
    LOG.warn(th, "Failed to download [%s]", segment.getId());
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
  public boolean finishRemove(SegmentId segmentId)
  {
    final AtomicBoolean result = new AtomicBoolean(true);

    segmentMetadataMap.compute(segmentId, (key, metadata) -> {
      if (null == metadata) {
        throw new ISE("Being asked to remove a segment [%s] that is not added yet", segmentId);
      }
      if (!metadata.isToBeRemoved()) {
        LOG.warn("[%s] was being asked to remove but is not marked for removal. " +
            "It might have been re-assigned back to this server. Skipping the remove operation", segmentId);
        result.set(false);
        return metadata;
      }
      VirtualReferenceCountingSegment segmentReference = metadata.getVirtualSegment();

      if (null != segmentReference) {
        if (segmentReference.hasActiveQueries()) {
          LOG.warn("Being asked to remove a segment [%s] that has active queries in progress", segmentId);
          //TODO: maybe throw segmentNotEvictableException and retry remove after caching the exception
        }
        try {
          // Do an actual close of segment. We are closing the segment here instead of doing it in the segment manager
          // since segment could be re-assigned even before we actually get to deleting the segment. If segment manager
          // closes the segment and segment gets re-assigned before being removed entirely, we end up with a virtual
          // segment whose base object is closed and we can't open it back. There could also be in-flight queries
          // referring to this virtual segment before segment is eligible for full removal.
          segmentReference.closeFully();
        }
        catch (Exception ex) {
          LOG.error(ex, "Failed to close the segment [%s]", segmentId);
        }
      }
      strategy.remove(segmentId);
      if (metadata.getStatus() == Status.QUEUED) {
        // This can happen if queries are cancelled but segment is remove entirely before the segment download is cancelled
        // fail the future with an exception.
        metadata.getDownloadFuture().setException(new ISE("[%s] Segment was removed from this machine", segmentId));
      }
      virtualSegmentsStats.incrementNumSegmentRemoved();
      return null;
    });
    return result.get();
  }

  @Override
  public void beginRemove(SegmentId segmentId)
  {
    segmentMetadataMap.compute(segmentId, (key, metadata) -> {
      if (null == metadata) {
        throw new ISE("Being asked to remove a segment [%s] that is not added yet", segmentId);
      }
      metadata.markToBeRemoved();
      return metadata;
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
    // Segment is ready to be used.
    READY,

    // Segment is queued for download.
    QUEUED,

    // Segment is downloaded to disk
    DOWNLOADED
  }
}
