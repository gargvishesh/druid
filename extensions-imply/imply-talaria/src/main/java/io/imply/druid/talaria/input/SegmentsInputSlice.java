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
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Objects;

/**
 * Input slice representing a set of segments to read.
 *
 * Sliced from {@link TableInputSpec} by {@link TableInputSpecSlicer}.
 *
 * Similar to {@link org.apache.druid.query.spec.MultipleSpecificSegmentSpec} from native queries.
 *
 * These use {@link RichSegmentDescriptor}, not {@link org.apache.druid.timeline.DataSegment}, to minimize overhead
 * in scenarios where the target server already has the segment cached. If the segment isn't cached, the target
 * server does need to fetch the full {@link org.apache.druid.timeline.DataSegment} object, so it can get the
 * {@link org.apache.druid.segment.loading.LoadSpec} and fetch the segment from deep storage.
 */
@JsonTypeName("segments")
public class SegmentsInputSlice implements InputSlice
{
  private final String dataSource;
  private final List<RichSegmentDescriptor> descriptors;

  @JsonCreator
  public SegmentsInputSlice(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<RichSegmentDescriptor> descriptors
  )
  {
    this.dataSource = dataSource;
    this.descriptors = descriptors;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("segments")
  public List<RichSegmentDescriptor> getDescriptors()
  {
    return descriptors;
  }

  @Override
  public int numFiles()
  {
    return descriptors.size();
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
    SegmentsInputSlice that = (SegmentsInputSlice) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(descriptors, that.descriptors);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, descriptors);
  }

  @Override
  public String toString()
  {
    return "SegmentsInputSlice{" +
           "dataSource='" + dataSource + '\'' +
           ", descriptors=" + descriptors +
           '}';
  }
}
