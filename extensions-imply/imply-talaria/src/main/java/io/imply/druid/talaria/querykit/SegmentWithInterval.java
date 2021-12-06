/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Objects;

public class SegmentWithInterval implements Closeable
{
  private final LazyResourceHolder<? extends Segment> segmentHolder;
  private final SegmentId segmentId;
  private final Interval interval;

  public SegmentWithInterval(
      final LazyResourceHolder<? extends Segment> segmentHolder,
      final SegmentId segmentId,
      final Interval interval
  )
  {
    this.segmentHolder = Preconditions.checkNotNull(segmentHolder, "segment");
    this.segmentId = Preconditions.checkNotNull(segmentId, "segmentId");
    this.interval = Preconditions.checkNotNull(interval, "interval");

    if (!segmentId.getInterval().contains(interval)) {
      throw new IAE("Segment [%s] does not contain interval [%s]", segmentId.toString(), interval);
    }
  }

  public Segment getOrLoadSegment()
  {
    return segmentHolder.get();
  }

  @Override
  public void close()
  {
    segmentHolder.close();
  }

  public SegmentDescriptor toDescriptor()
  {
    return new SegmentDescriptor(interval, segmentId.getVersion(), segmentId.getPartitionNum());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentWithInterval that = (SegmentWithInterval) o;
    return Objects.equals(segmentHolder, that.segmentHolder) && Objects.equals(interval, that.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentHolder, interval);
  }

  @Override
  public String toString()
  {
    return "SegmentWithInterval{" +
           "segment=" + segmentId +
           ", interval=" + interval +
           '}';
  }
}
