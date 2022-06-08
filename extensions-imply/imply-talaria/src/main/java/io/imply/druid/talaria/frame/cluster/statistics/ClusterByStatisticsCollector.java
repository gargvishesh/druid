/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;

/**
 * Collects statistics that help determine how to optimally partition a dataset to achieve a desired {@link ClusterBy}.
 *
 * Not thread-safe.
 */
public interface ClusterByStatisticsCollector
{
  /**
   * Returns the {@link ClusterBy} that this collector uses to bucket and sort keys.
   */
  ClusterBy getClusterBy();

  /**
   * Add a key to this collector.
   *
   * The "weight" parameter must be a positive integer. It should be 1 for "normal" reasonably-sized rows, and a
   * larger-than-1-but-still-small integer for "jumbo" rows. This allows {@link #generatePartitionsWithTargetWeight}
   * to behave reasonably when passed a row count for the target weight: if all rows are reasonably sized, weight
   * is equivalent to rows; however, if rows are jumbo then the generated partition ranges will have fewer rows to
   * accommodate the extra weight.
   */
  ClusterByStatisticsCollector add(ClusterByKey key, int weight);

  /**
   * Add another collector's data to this collector. Does not modify the other collector.
   */
  ClusterByStatisticsCollector addAll(ClusterByStatisticsCollector other);

  /**
   * Add a snapshot to this collector.
   */
  ClusterByStatisticsCollector addAll(ClusterByStatisticsSnapshot other);

  /**
   * Estimated total amount of row weight in the dataset, based on what keys have been added so far.
   */
  long estimatedTotalWeight();

  /**
   * Whether this collector has encountered any multi-valued input at a particular key position.
   *
   * This method exists because {@link org.apache.druid.timeline.partition.DimensionRangeShardSpec} does not
   * support partitioning on multi-valued strings, so we need to know if any multi-valued strings exist in order
   * to decide whether we can use this kind of shard spec.
   *
   * @throws IllegalArgumentException if keyPosition is outside the range of {@link #getClusterBy()}
   * @throws IllegalStateException    if this collector was not checking keys for multiple-values
   */
  boolean hasMultipleValues(int keyPosition);

  /**
   * Removes all data from this collector.
   */
  ClusterByStatisticsCollector clear();

  /**
   * Generates key ranges, targeting a particular row weight per range. The actual amount of row weight per range
   * may be higher or lower than the provided target.
   */
  ClusterByPartitions generatePartitionsWithTargetWeight(long targetWeight);

  /**
   * Generates up to "maxNumPartitions" key ranges. The actual number of generated partitions may be less than the
   * provided maximum.
   */
  ClusterByPartitions generatePartitionsWithMaxCount(int maxNumPartitions);

  /**
   * Returns an immutable, JSON-serializable snapshot of this collector.
   */
  ClusterByStatisticsSnapshot snapshot();
}
