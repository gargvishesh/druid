/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.Objects;

public class MaxCountShuffleSpec implements ShuffleSpec
{
  private final ClusterBy clusterBy;
  private final int partitions;
  private final boolean aggregate;

  @JsonCreator
  public MaxCountShuffleSpec(
      @JsonProperty("clusterBy") final ClusterBy clusterBy,
      @JsonProperty("partitions") final int partitions,
      @JsonProperty("aggregate") final boolean aggregate
  )
  {
    this.clusterBy = Preconditions.checkNotNull(clusterBy, "clusterBy");
    this.partitions = partitions;
    this.aggregate = aggregate;

    if (partitions < 1) {
      throw new IAE("Partition count must be at least 1");
    }
  }

  @Override
  @JsonProperty("aggregate")
  public boolean doesAggregateByClusterKey()
  {
    return aggregate;
  }

  @Override
  public boolean needsStatistics()
  {
    return partitions > 1 || clusterBy.getBucketByCount() > 0;
  }

  @Override
  public Either<Long, ClusterByPartitions> generatePartitions(
      @Nullable final ClusterByStatisticsCollector collector,
      final int maxNumPartitions
  )
  {
    if (!needsStatistics()) {
      return Either.value(ClusterByPartitions.oneUniversalPartition());
    } else if (partitions > maxNumPartitions) {
      return Either.error((long) partitions);
    } else {
      final ClusterByPartitions generatedPartitions = collector.generatePartitionsWithMaxCount(partitions);
      if (generatedPartitions.size() <= maxNumPartitions) {
        return Either.value(generatedPartitions);
      } else {
        return Either.error((long) generatedPartitions.size());
      }
    }
  }

  @Override
  @JsonProperty
  public ClusterBy getClusterBy()
  {
    return clusterBy;
  }

  @JsonProperty
  int getPartitions()
  {
    return partitions;
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
    MaxCountShuffleSpec that = (MaxCountShuffleSpec) o;
    return partitions == that.partitions
           && aggregate == that.aggregate
           && Objects.equals(clusterBy, that.clusterBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusterBy, partitions, aggregate);
  }

  @Override
  public String toString()
  {
    return "MaxCountShuffleSpec{" +
           "clusterBy=" + clusterBy +
           ", partitions=" + partitions +
           ", aggregate=" + aggregate +
           '}';
  }
}
