/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;


import org.apache.druid.timeline.SegmentId;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * This is a very simple implementation of {@link SegmentReplacementStrategy} that is used to decide which segment
 * to download and which segment to evict.
 */
public class FIFOSegmentReplacementStrategy implements SegmentReplacementStrategy
{
  private final Queue<SegmentId> processQueue;
  private final Queue<QueueItem> downloadedSegments;
  private static final String METRIC_PREFIX = "virtual/segments/queue/";

  public FIFOSegmentReplacementStrategy()
  {
    processQueue = new ConcurrentLinkedDeque<>();
    downloadedSegments = new ConcurrentLinkedDeque<>();
  }

  @Override
  public SegmentId nextProcess()
  {
    return processQueue.poll();
  }

  @Override
  public SegmentId nextEvict()
  {
    for (QueueItem item : downloadedSegments) {
      if (!item.metadata.getVirtualSegment().hasActiveQueries()) {
        return item.segmentId;
      }
    }
    return null;
  }

  @Override
  public void downloaded(SegmentId segmentId, VirtualSegmentMetadata metadata)
  {
    downloadedSegments.add(new QueueItem(segmentId, metadata));
  }

  @Override
  public synchronized void queue(SegmentId segmentId, VirtualSegmentMetadata metadata)
  {
    if (processQueue.contains(segmentId)) {
      //TODO: More performant implementation
      return;
    }
    processQueue.add(segmentId);
  }

  @Override
  public void remove(SegmentId segmentId)
  {
    processQueue.remove(segmentId);
    downloadedSegments.removeIf(item -> item.segmentId.equals(segmentId));
  }

  @Override
  public Map<String, Number> getMetrics()
  {
    Map<String, Number> metrics = new HashMap<>();
    metrics.put(METRIC_PREFIX + "toDownload", processQueue.size());
    metrics.put(METRIC_PREFIX + "downloaded", downloadedSegments.size());
    return metrics;
  }

  //TODO: ideally, the check that we should not evict a current used segment should be outside this class
  private static class QueueItem
  {
    private SegmentId segmentId;
    private VirtualSegmentMetadata metadata;

    QueueItem(SegmentId segmentId, VirtualSegmentMetadata metadata)
    {
      this.segmentId = segmentId;
      this.metadata = metadata;
    }
  }
}
