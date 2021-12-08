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
   */
  ClusterByStatisticsCollector add(ClusterByKey key);

  /**
   * Add another collector's data to this collector. Does not modify the other collector.
   */
  ClusterByStatisticsCollector addAll(ClusterByStatisticsCollector other);

  /**
   * Add a snapshot to this collector.
   */
  ClusterByStatisticsCollector addAll(ClusterByStatisticsSnapshot other);

  /**
   * Estimated number of rows in the dataset, based on what keys have been added so far.
   */
  long estimatedCount();

  /**
   * Whether this collector has encountered any multi-valued input at a particular key position.
   *
   * @throws IllegalArgumentException if keyPosition is outside the range of {@link #getClusterBy()}
   */
  boolean hasMultipleValues(int keyPosition);

  /**
   * Removes all data from this collector.
   */
  ClusterByStatisticsCollector clear();

  /**
   * Generates key ranges, targeting a particular number of rows per range. The actual number of rows per range
   * may be higher or lower than the provided target.
   */
  ClusterByPartitions generatePartitionsWithTargetSize(long targetPartitionSize);

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
