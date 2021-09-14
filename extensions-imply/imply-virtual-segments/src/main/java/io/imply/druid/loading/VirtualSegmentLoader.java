/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.name.Named;
import io.imply.druid.VirtualSegmentConfig;
import io.imply.druid.segment.SegmentNotEvictableException;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegment;
import io.imply.druid.segment.VirtualSegmentStateManager;
import io.imply.druid.segment.VirtualSegmentStats;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bridge between {@link SegmentLoader} and {@link org.apache.druid.server.SegmentManager}. It
 * manages the lifecycle of virtual segments. A virtual segment is a logical segment that may not have been downloaded
 * yet. Virtual segment can still be passed around however it can't be queried unless it is downloaded on the disk. It
 * also relies on the {@link VirtualSegmentStateManager} for atomic updates of segment state.
 */
@ManageLifecycle
public class VirtualSegmentLoader implements SegmentLoader
{
  private static final Logger LOG = new Logger(VirtualSegmentLoader.class);
  private final ScheduledThreadPoolExecutor downloadExecutor;
  private final ScheduledThreadPoolExecutor cleanupExecutor;
  private final VirtualSegmentStateManager segmentStateManager;
  private final SegmentLocalCacheManager physicalCacheManager;
  // Size of the location with largest space
  private final long maxSizeLocation;
  private final VirtualSegmentConfig config;
  private final ReentrantLock lock = new ReentrantLock();
  private final SegmentizerFactory defaultSegmentizerFactory;
  private final ObjectMapper jsonMapper;
  private final VirtualSegmentStats virtualSegmentStats;
  private volatile boolean started = false;
  public static final String DOWNLOAD_METRIC_PREFIX = "virtual/segment/download/pool/";
  public static final String CLEANUP_METRIC_PREFIX = "virtual/segment/cleanup/pool/";


  @Inject
  public VirtualSegmentLoader(
      final SegmentLocalCacheManager physicalCacheManager,
      final SegmentLoaderConfig segmentLoaderConfig,
      final VirtualSegmentStateManager segmentStateManager,
      final VirtualSegmentConfig config,
      final VirtualSegmentStats virtualSegmentStats,
      final IndexIO indexIO,
      @Named("virtualSegmentCleanup") final ScheduledThreadPoolExecutor cleanupExecutor,
      @Json final ObjectMapper mapper
  )
  {
    this(
        physicalCacheManager,
        segmentLoaderConfig,
        segmentStateManager,
        config,
        mapper,
        new MMappedQueryableSegmentizerFactory(indexIO),
        virtualSegmentStats,
        cleanupExecutor
    );
  }

  @VisibleForTesting
  public VirtualSegmentLoader(
      final SegmentLocalCacheManager physicalCacheManager,
      final SegmentLoaderConfig segmentLoaderConfig,
      final VirtualSegmentStateManager segmentStateManager,
      final VirtualSegmentConfig config,
      final ObjectMapper mapper,
      final SegmentizerFactory defaultSegmentizerFactory,
      final VirtualSegmentStats virtualSegmentStats,
      final ScheduledThreadPoolExecutor cleanupExecutor
  )
  {
    Preconditions.checkArgument(
        segmentLoaderConfig.isDeleteOnRemove(),
        "For virtual segments to work, druid.segmentCache.deleteOnRemove must be set to true"
    );
    // TODO: Take queue size as an input in the virtual segment config. Default to infinite queue.q
    downloadExecutor = new ScheduledThreadPoolExecutor(
        config.getDownloadThreads(),
        Execs.makeThreadFactory("virtual-segment-download-%d")
    );

    this.segmentStateManager = segmentStateManager;
    this.physicalCacheManager = physicalCacheManager;
    this.maxSizeLocation = segmentLoaderConfig.getLocations()
                                              .stream()
                                              .mapToLong(StorageLocationConfig::getMaxSize)
                                              .max()
                                              .orElseThrow(() -> new IAE("No locations configured"));
    this.config = config;
    this.jsonMapper = mapper;
    this.defaultSegmentizerFactory = defaultSegmentizerFactory;
    this.virtualSegmentStats = virtualSegmentStats;
    this.cleanupExecutor = cleanupExecutor;
  }

  @LifecycleStart
  public synchronized void start()
  {
    if (started) {
      LOG.info("Already started but asked to start again");
      return;
    }

    LOG.info("Starting scheduled downloads");
    for (int i = 0; i < config.getDownloadThreads(); i++) {
      LOG.info("Scheduling downloads tasks %d", i);
      downloadExecutor.scheduleWithFixedDelay(
          this::downloadNextSegment,
          config.getDownloadDelayMs(),
          config.getDownloadDelayMs(),
          TimeUnit.MILLISECONDS
      );
    }
    started = true;
  }

  @LifecycleStop
  public synchronized void stop()
  {
    if (!started) {
      LOG.info("Already stopped but being asked to stop again");
      return;
    }
    LOG.info("Stopping scheduled downloads");
    downloadExecutor.shutdown();
    cleanupExecutor.shutdown();
    started = false;
  }

  public void downloadNextSegment()
  {
    VirtualReferenceCountingSegment segmentReference = null;
    DataSegment dataSegment = null;
    boolean isReserved = false;

    while (true) {
      lock.lock();
      try {
        segmentReference = segmentStateManager.toDownload();

        // Do not continue the loop if there are no more segments to download
        if (null == segmentReference) {
          LOG.trace("No new segments to download");
          return;
        }

        // Skip the download of this segment if it has no active queries
        // and the cancel of download was successful
        if (!segmentReference.hasActiveQueries()
            && segmentStateManager.cancelDownload(segmentReference)) {
          // Continue with the next segment without waiting
          continue;
        }

        dataSegment = asDataSegment(segmentReference);

        // First we try to reserve the space
        // reserve the segment if it can
        isReserved = physicalCacheManager.reserve(dataSegment);
        while (!isReserved) {
          VirtualReferenceCountingSegment toEvict = segmentStateManager.toEvict();
          if (null == toEvict) {
            break;
          }
          try {
            evictSegment(toEvict);
          }
          catch (SegmentNotEvictableException sne) {
            LOG.info("[%s] in already in use", toEvict.getId());
            continue;
          }
          isReserved = physicalCacheManager.reserve(dataSegment);
        }

        if (!isReserved) {
          //TODO: maybe there are many segments in use so it can't be downloaded right now
          // Add a pause mechanism
          LOG.info("Failed to download [%s] since there is not enough space. Will try again", dataSegment.getId());
          segmentStateManager.downloadFailed(segmentReference, false);
          segmentStateManager.requeue(segmentReference);

          // Do not continue the loop because space is not available
          return;
        }
      }
      catch (Exception ex) {
        LOG.error(
            ex,
            "Failed to download the next segment [%s]",
            (null == segmentReference) ? null : segmentReference.getId()
        );
        if (isReserved) {
          // If we reserved the storage but then ran into an error, we need to un-reserve the segment
          //if SegmentLoaderConfig#isDeleteOnRemove() is set to false, it wouldn't actually delete the files
          physicalCacheManager.cleanup(dataSegment);
          physicalCacheManager.release(dataSegment);
        }
        if (null != segmentReference) {
          SegmentLifecycleLogger.LOG.debug("Rescheduling [%s] for re-download after failure", segmentReference.getId());
          segmentStateManager.requeue(segmentReference);
        }
        return;
      }
      finally {
        lock.unlock();
      }

      // Do the download
      try {
        materializeSegment(segmentReference, dataSegment);
      }
      catch (SegmentLoadingException e) {
        LOG.error(e, "Failed to materialize the segment [%s]", dataSegment.getId());
        //TODO: pause
        segmentStateManager.requeue(segmentReference);
      }
    }
  }

  /**
   * Evicts the segment and cleans up the data but does not deregister the segment from the historical.
   *
   * @param segment Segment to evict and remove from disk
   */
  public void evictSegment(VirtualReferenceCountingSegment segment)
  {
    DataSegment dataSegment = asDataSegment(segment);
    segmentStateManager.evict(segment);
    physicalCacheManager.cleanup(dataSegment);
  }

  /**
   * Marks a segment for download.
   *
   * @param segment - Segment to schedule the download for.
   * @param closer  - once the segment is no longer in need by the caller, this closer will be closed.
   * @return - Future object which is completed successfully if segment is downloaed or fails with an error
   */
  public ListenableFuture<Void> scheduleDownload(VirtualReferenceCountingSegment segment, Closer closer)
  {
    if (!started) {
      throw new ISE("cannot schedule a download unless this component is started");
    }
    return segmentStateManager.queue(segment, closer);
  }

  @Override
  public ReferenceCountingSegment getSegment(DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed)
      throws SegmentLoadingException
  {
    // getSegment can be called multiple times for same DataSegment.
    //TODO: locks
    //TODO: some segments are eagerly materialized
    //TODO: atomic timeline insertion
    //TODO: make sure that materializeSegment is called only once
    VirtualReferenceCountingSegment segmentReference = VirtualReferenceCountingSegment.wrapSegment(
        new VirtualSegment(segment, lazy, loadFailed),
        segment.getShardSpec()
    );
    VirtualReferenceCountingSegment previous = segmentStateManager.registerIfAbsent(segmentReference);
    if (previous != null) {
      // If there is an existing virtual segment registered, return that
      return previous;
    }
    if (segment.getSize() > maxSizeLocation) {
      throw new IAE(
          "Cannot fit segment [%s] with size [%d]. Max size possible: [%d]",
          segment.getId(),
          segment.getSize(),
          maxSizeLocation
      );
    }

    // This snippet cannot be called multiple times for same data segment. This could mean that we are
    // creating multiple segment references for same underying segment. It is possible in such a situation
    // that a reference is acquired on one segment reference but released on other reference.
    boolean isCached = physicalCacheManager.isSegmentCached(segment);
    if (isCached) {
      materializeSegment(segmentReference, segment);
    }
    return segmentReference;
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    final SegmentId segmentId = segment.getId();
    segmentStateManager.beginRemove(segmentId);

    // Once a segment has been marked to be removed, the reference count of a segment will only decrease or stay at zero
    // unless segment gets re-assigned.
    VirtualReferenceCountingSegment segmentReference = segmentStateManager.get(segmentId);
    if (null != segmentReference) {
      // callback should be quick and submit a task to cleanup threadpool.
      segmentReference.whenNotActive(() -> cleanupExecutor.execute(new SegmentCleanupRunnable(segment)));
    }
  }

  public Map<String, Number> getWorkersGauges()
  {
    final Map<String, Number> qMetricsMap = new HashMap<>();
    qMetricsMap.put(DOWNLOAD_METRIC_PREFIX + "workers", downloadExecutor.getCorePoolSize());
    qMetricsMap.put(CLEANUP_METRIC_PREFIX + "pending", cleanupExecutor.getQueue().size());
    return qMetricsMap;
  }

  //TODO: maybe this should be called in {SegmentStateManager.compute()] so its called serially for a segment
  private void materializeSegment(VirtualReferenceCountingSegment segmentReference, DataSegment dataSegment)
      throws SegmentLoadingException
  {
    VirtualSegment segment = (VirtualSegment) segmentReference.getBaseSegment();
    Preconditions.checkNotNull(segment, "Null base segment [%s]", segmentReference.getId());
    Segment realSegment = loadRealSegment(dataSegment, segment.isLazy(), segment.getLoadFailedCallback());
    segment.setRealSegment(realSegment);
    segmentStateManager.downloaded(segmentReference);
  }

  private DataSegment asDataSegment(VirtualReferenceCountingSegment segment)
  {
    Segment baseSegment = segment.getBaseSegment();
    if (!(baseSegment instanceof VirtualSegment)) {
      throw new IAE(
          "Was expecting a virtual segment as base segment but found [%s]",
          (baseSegment == null) ? null : baseSegment.getClass()
      );
    }
    return ((VirtualSegment) baseSegment).asDataSegment();
  }

  private Segment loadRealSegment(DataSegment dataSegment, boolean lazy, SegmentLazyLoadFailCallback loadFailed)
      throws SegmentLoadingException
  {
    // download metrics
    long startDownloadTime = System.nanoTime();
    File segmentFiles = physicalCacheManager.getSegmentFiles(dataSegment);
    virtualSegmentStats.recordDownloadTime(
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startDownloadTime), dataSegment.getSize());

    File factoryJson = new File(segmentFiles, "factory.json");
    final SegmentizerFactory factory;

    if (factoryJson.exists()) {
      try {
        factory = jsonMapper.readValue(factoryJson, SegmentizerFactory.class);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "%s", e.getMessage());
      }
    } else {
      factory = defaultSegmentizerFactory;
    }

    return factory.factorize(dataSegment, segmentFiles, lazy, loadFailed);
  }

  private final class SegmentCleanupRunnable implements Runnable
  {
    private final DataSegment dataSegment;

    public SegmentCleanupRunnable(DataSegment dataSegment)
    {
      this.dataSegment = dataSegment;
    }

    @Override
    public void run()
    {
      SegmentId segmentId = dataSegment.getId();
      try {
        lock.lock();
        VirtualReferenceCountingSegment segmentReference = segmentStateManager.get(segmentId);
        if (null == segmentReference) {
          LOG.error("an entry in state manager was expected for segment [%s]", segmentId);
          return;
        }
        if (segmentStateManager.finishRemove(segmentId)) {
          // do the actual cleanup if the segment was successfully removed.
          // the cleanup below won't race with download or materialization since we are taking a lock
          // cleanup is only done if the segment was cached. Though we release the location in any case
          // in case we reserved it due to a bug.
          if (physicalCacheManager.isSegmentCached(dataSegment)) {
            physicalCacheManager.cleanup(dataSegment);
          }
          physicalCacheManager.release(dataSegment);
        }
      }
      finally {
        lock.unlock();
      }
    }
  }
}
