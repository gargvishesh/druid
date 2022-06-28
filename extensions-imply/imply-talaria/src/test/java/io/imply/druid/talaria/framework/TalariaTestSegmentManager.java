/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Segment manager for tests to retrieve the generated segments in case of an insert query
 */
public class TalariaTestSegmentManager
{
  protected final Map<SegmentId, DataSegment> dataSegments = new ConcurrentHashMap<>();
  private final Map<SegmentId, Segment> segments = new ConcurrentHashMap<>();
  private final SegmentCacheManager segmentCacheManager;
  private final IndexIO indexIO;

  final Object lock = new Object();


  public TalariaTestSegmentManager(SegmentCacheManager segmentCacheManager, IndexIO indexIO)
  {
    this.segmentCacheManager = segmentCacheManager;
    this.indexIO = indexIO;
  }

  public void addDataSegment(DataSegment dataSegment)
  {
    synchronized (lock) {
      dataSegments.put(dataSegment.getId(), dataSegment);

      try {
        segmentCacheManager.getSegmentFiles(dataSegment);
      }
      catch (SegmentLoadingException e) {
        throw new ISE(e, "Unable to load segment [%s]", dataSegment.getId());
      }
    }
  }

  public Collection<DataSegment> getAllDataSegments()
  {
    return dataSegments.values();
  }

  public Collection<SegmentId> getAllDataSegmentIds()
  {
    return dataSegments.keySet();
  }

  public void addSegment(Segment segment)
  {
    segments.put(segment.getId(), segment);
  }

  @Nullable
  public Segment getSegment(SegmentId segmentId)
  {
    return segments.get(segmentId);
  }

}
