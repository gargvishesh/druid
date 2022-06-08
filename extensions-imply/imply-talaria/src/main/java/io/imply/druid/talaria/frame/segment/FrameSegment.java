/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * A {@link Segment} implementation based on a single {@link Frame}.
 *
 * This class is used for both columnar and row-based frames.
 */
public class FrameSegment implements Segment
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final SegmentId segmentId;

  public FrameSegment(Frame frame, FrameReader frameReader, SegmentId segmentId)
  {
    this.frame = frame;
    this.frameReader = frameReader;
    this.segmentId = segmentId;
  }

  @Override
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  public Interval getDataInterval()
  {
    return segmentId.getInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return new FrameStorageAdapter(frame, frameReader, segmentId.getInterval());
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
