/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.Objects;

/**
 * TODO(gianm): get rid of this?
 */
public class DataSegmentWithInterval
{
  private final DataSegment segment;
  private final Interval interval;

  @JsonCreator
  public DataSegmentWithInterval(
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("interval") Interval interval
  )
  {
    this.segment = Preconditions.checkNotNull(segment, "segment");
    this.interval = Preconditions.checkNotNull(interval, "interval");
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty("interval")
  public Interval getQueryInterval()
  {
    return interval;
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
    DataSegmentWithInterval that = (DataSegmentWithInterval) o;
    return Objects.equals(segment, that.segment) && Objects.equals(interval, that.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment, interval);
  }

  @Override
  public String toString()
  {
    return "DataSegmentPlusInterval{" +
           "segment=" + segment +
           ", interval=" + interval +
           '}';
  }
}
