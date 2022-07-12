/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Like {@link SegmentDescriptor}, but provides both the full interval and the clipped interval for a segment.
 * (SegmentDescriptor only provides the clipped interval.)
 *
 * To keep the serialized form lightweight, the full interval is only serialized if it is different from the
 * clipped interval.
 */
public class RichSegmentDescriptor extends SegmentDescriptor
{
  private final Interval fullInterval;

  public RichSegmentDescriptor(
      final Interval fullInterval,
      final Interval interval,
      final String version,
      final int partitionNumber
  )
  {
    super(interval, version, partitionNumber);
    this.fullInterval = Preconditions.checkNotNull(fullInterval, "fullInterval");
  }

  @JsonCreator
  static RichSegmentDescriptor fromJson(
      @JsonProperty("fi") @Nullable final Interval fullInterval,
      @JsonProperty("itvl") final Interval interval,
      @JsonProperty("ver") final String version,
      @JsonProperty("part") final int partitionNumber
  )
  {
    return new RichSegmentDescriptor(
        fullInterval != null ? fullInterval : interval,
        interval,
        version,
        partitionNumber
    );
  }

  @JsonProperty("fi")
  public Interval getFullInterval()
  {
    return fullInterval;
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
    if (!super.equals(o)) {
      return false;
    }
    RichSegmentDescriptor that = (RichSegmentDescriptor) o;
    return Objects.equals(fullInterval, that.fullInterval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), fullInterval);
  }

  @Override
  public String toString()
  {
    return "RichSegmentDescriptor{" +
           "fullInterval=" + fullInterval +
           ", interval=" + getInterval() +
           ", version='" + getVersion() + '\'' +
           ", partitionNumber=" + getPartitionNumber() +
           '}';
  }
}
