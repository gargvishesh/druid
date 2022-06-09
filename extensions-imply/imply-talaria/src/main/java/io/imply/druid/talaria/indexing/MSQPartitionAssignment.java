/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

import java.util.Map;
import java.util.Objects;

public class MSQPartitionAssignment
{
  private final ClusterByPartitions partitions;
  private final Map<Integer, SegmentIdWithShardSpec> allocations;

  @JsonCreator
  public MSQPartitionAssignment(
      @JsonProperty("partitions") ClusterByPartitions partitions,
      @JsonProperty("allocations") Map<Integer, SegmentIdWithShardSpec> allocations
  )
  {
    this.partitions = Preconditions.checkNotNull(partitions, "partitions");
    this.allocations = allocations;

    // Sanity checks.
    for (final int partitionNumber : allocations.keySet()) {
      if (partitionNumber < 0 || partitionNumber >= partitions.size()) {
        throw new IAE("Partition [%s] out of bounds", partitionNumber);
      }
    }
  }

  @JsonProperty
  public ClusterByPartitions getPartitions()
  {
    return partitions;
  }

  @JsonProperty
  public Map<Integer, SegmentIdWithShardSpec> getAllocations()
  {
    return allocations;
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
    MSQPartitionAssignment that = (MSQPartitionAssignment) o;
    return Objects.equals(partitions, that.partitions) && Objects.equals(
        allocations,
        that.allocations
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitions, allocations);
  }

  @Override
  public String toString()
  {
    return "MSQPartitionAssignment{" +
           "partitions=" + partitions +
           ", allocations=" + allocations +
           '}';
  }
}
