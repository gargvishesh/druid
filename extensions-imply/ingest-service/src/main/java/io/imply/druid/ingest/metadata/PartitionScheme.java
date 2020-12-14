/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nullable;
import java.util.Objects;

public class PartitionScheme
{
  @Nullable
  private final Granularity segmentGranularity; // primary time-based range partitioning
  @Nullable
  private final PartitionsSpec secondaryPartitionsSpec; // this is not used yet, but maybe someday.

  @JsonCreator
  public PartitionScheme(
      @JsonProperty("segmentGranularity") @Nullable Granularity segmentGranularity,
      @JsonProperty("secondaryPartitionsSpec") @Nullable PartitionsSpec secondaryPartitionsSpec
  )
  {
    this.segmentGranularity = segmentGranularity;
    this.secondaryPartitionsSpec = secondaryPartitionsSpec;
  }

  @Nullable
  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Nullable
  @JsonProperty
  public PartitionsSpec getSecondaryPartitionsSpec()
  {
    return secondaryPartitionsSpec;
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
    PartitionScheme that = (PartitionScheme) o;
    return Objects.equals(segmentGranularity, that.segmentGranularity) &&
           Objects.equals(secondaryPartitionsSpec, that.secondaryPartitionsSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentGranularity, secondaryPartitionsSpec);
  }
}
