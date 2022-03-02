/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;

public interface KeyCollector<CollectorType extends KeyCollector<CollectorType>>
{
  /**
   * Add a key with a certain weight to this collector.
   *
   * See {@link ClusterByStatisticsCollector#add} for the meaning of "weight".
   */
  void add(ClusterByKey key, long weight);

  /**
   * Fold another collector into this one.
   */
  void addAll(CollectorType other);

  /**
   * Returns whether this collector is empty.
   */
  boolean isEmpty();

  /**
   * Returns an estimate of the amount of total weight currently tracked by this collector. This may change over
   * time as more keys are added.
   */
  long estimatedTotalWeight();

  /**
   * Returns an estimate of the number of keys currently retained by this collector. This may change over time as
   * more keys are added.
   */
  int estimatedRetainedKeys();

  /**
   * Downsample this collector, dropping about half of the keys that are currently retained. Returns true if
   * the collector was downsampled, or if it is already retaining zero or one keys. Returns false if the collector is
   * retaining more than one key, yet cannot be downsampled any further.
   */
  boolean downSample();

  /**
   * Returns the minimum key encountered by this collector so far, if any have been encountered.
   *
   * @throws java.util.NoSuchElementException if the collector is empty; i.e. if {@link #isEmpty()} is true.
   */
  ClusterByKey minKey();

  /**
   * Generates key ranges, targeting a particular row weight per range.
   *
   * @param targetWeight row weight per partition. The actual amount of row weight per range may be higher
   *                     or lower than the provided target.
   */
  ClusterByPartitions generatePartitionsWithTargetWeight(long targetWeight);
}
