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

import javax.annotation.Nullable;
import java.util.Objects;

public class TargetSizeShuffleSpec implements ShuffleSpec
{
  private final ClusterBy clusterBy;
  private final long targetSize;
  private final boolean aggregate;

  @JsonCreator
  public TargetSizeShuffleSpec(
      @JsonProperty("clusterBy") final ClusterBy clusterBy,
      @JsonProperty("targetSize") final long targetSize,
      @JsonProperty("aggregate") final boolean aggregate
  )
  {
    this.clusterBy = Preconditions.checkNotNull(clusterBy, "clusterBy");
    this.targetSize = targetSize;
    this.aggregate = aggregate;
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
    return true;
  }

  @Override
  public Either<Long, ClusterByPartitions> generatePartitions(
      @Nullable final ClusterByStatisticsCollector collector,
      final int maxNumPartitions
  )
  {
    final long expectedPartitions = collector.estimatedCount() / targetSize;

    if (expectedPartitions > maxNumPartitions) {
      return Either.error(expectedPartitions);
    } else {
      final ClusterByPartitions generatedPartitions = collector.generatePartitionsWithTargetSize(targetSize);
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
  long getTargetSize()
  {
    return targetSize;
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
    TargetSizeShuffleSpec that = (TargetSizeShuffleSpec) o;
    return targetSize == that.targetSize && aggregate == that.aggregate && Objects.equals(clusterBy, that.clusterBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusterBy, targetSize, aggregate);
  }

  @Override
  public String toString()
  {
    return "TargetSizeShuffleSpec{" +
           "clusterBy=" + clusterBy +
           ", targetSize=" + targetSize +
           ", aggregate=" + aggregate +
           '}';
  }
}
