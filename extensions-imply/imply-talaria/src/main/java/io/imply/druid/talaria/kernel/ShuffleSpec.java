/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import org.apache.druid.java.util.common.Either;

import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "maxCount", value = MaxCountShuffleSpec.class),
    @JsonSubTypes.Type(name = "targetSize", value = TargetSizeShuffleSpec.class)
})
public interface ShuffleSpec
{
  /**
   * The clustering instructions that will determine how data are shuffled.
   */
  ClusterBy getClusterBy();

  /**
   * Whether this stage aggregates by the clustering key or not.
   */
  boolean doesAggregateByClusterKey();

  /**
   * Whether {@link #generatePartitions} needs a nonnull collector.
   */
  boolean needsStatistics();

  /**
   * Generates a set of partitions based on the provided statistics.
   *
   * @param collector        must be nonnull if {@link #needsStatistics()} is true; may be null otherwise
   * @param maxNumPartitions maximum number of partitions to generate
   *
   * @return either the partition assignment, or (as an error) a number of partitions, greater than maxNumPartitions,
   * that would be expected to be created
   */
  Either<Long, ClusterByPartitions> generatePartitions(
      @Nullable ClusterByStatisticsCollector collector,
      int maxNumPartitions
  );
}
