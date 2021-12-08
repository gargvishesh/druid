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
  void add(ClusterByKey key);

  void addAll(CollectorType other);

  long estimatedCount();

  int estimatedRetainedKeys();

  /**
   * Downsample this collector, dropping about half of the keys that are currently being tracked.
   *
   * @throws IllegalStateException if the collector cannot be downsampled any further
   */
  void downSample();

  /**
   * Returns the minimum key encountered by this collector so far.
   *
   * @throws java.util.NoSuchElementException if the collector is empty
   */
  ClusterByKey minKey();

  ClusterByPartitions generatePartitionsWithTargetSize(long targetPartitionSize);
}
