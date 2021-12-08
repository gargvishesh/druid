/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class DistinctKeyCollector implements KeyCollector<DistinctKeyCollector>
{
  private static final int INITIAL_MAX_KEYS = 2 << 16 /* 131,072 */;

  private final Comparator<ClusterByKey> comparator;
  private final TreeSet<ClusterByKey> retainedKeys;
  private int maxKeys;

  // Reduce the keyspace by 2 to the power of this factor.
  private int spaceReductionFactor;

  DistinctKeyCollector(
      final Comparator<ClusterByKey> comparator,
      final TreeSet<ClusterByKey> retainedKeys,
      final int spaceReductionFactor
  )
  {
    this.comparator = Preconditions.checkNotNull(comparator, "comparator");
    this.retainedKeys = Preconditions.checkNotNull(retainedKeys, "retainedKeys");
    this.maxKeys = INITIAL_MAX_KEYS;
    this.spaceReductionFactor = spaceReductionFactor;
  }

  DistinctKeyCollector(final Comparator<ClusterByKey> comparator)
  {
    this(comparator, new TreeSet<>(comparator), 0);
  }

  @Override
  public void add(ClusterByKey key)
  {
    final boolean isNewMin = retainedKeys.isEmpty() || comparator.compare(key, retainedKeys.first()) < 0;

    if (isNewMin || isKeySelected(key)) {
      if (isNewMin && !retainedKeys.isEmpty() && !isKeySelected(retainedKeys.first())) {
        // Old min should be kicked out.
        retainedKeys.pollFirst();
      }

      retainedKeys.add(key);

      while (retainedKeys.size() >= maxKeys) {
        increaseSpaceReductionFactor();
      }
    }
  }

  @Override
  public void addAll(DistinctKeyCollector other)
  {
    while (!retainedKeys.isEmpty() && spaceReductionFactor < other.spaceReductionFactor) {
      increaseSpaceReductionFactor();
    }

    if (retainedKeys.isEmpty()) {
      this.spaceReductionFactor = other.spaceReductionFactor;
    }

    for (final ClusterByKey key : other.retainedKeys) {
      add(key);
    }
  }

  @Override
  public long estimatedCount()
  {
    return (long) retainedKeys.size() << spaceReductionFactor;
  }

  @Override
  public int estimatedRetainedKeys()
  {
    return retainedKeys.size();
  }

  @Override
  public ClusterByKey minKey()
  {
    return retainedKeys.first();
  }

  @Override
  public void downSample()
  {
    if (maxKeys == 1) {
      throw new ISE("Cannot downsample any further");
    }

    maxKeys /= 2;

    while (retainedKeys.size() >= maxKeys) {
      increaseSpaceReductionFactor();
    }
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetSize(final long targetPartitionSize)
  {
    if (targetPartitionSize < 1) {
      throw new IAE("targetPartitionSize must be positive");
    } else if (retainedKeys.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final long keyWeight = 1L << spaceReductionFactor;
    final List<ClusterByPartition> partitions = new ArrayList<>();
    final Iterator<ClusterByKey> iterator = retainedKeys.iterator();
    ClusterByKey startKey = retainedKeys.first();
    long partitionCount = 0;

    while (iterator.hasNext()) {
      final ClusterByKey key = iterator.next();
      final long partitionCountAfterKey = partitionCount + keyWeight;

      if (partitionCount > 0
          && partitionCountAfterKey > targetPartitionSize
          && partitionCountAfterKey - targetPartitionSize > targetPartitionSize - partitionCount) {
        // New partition *not* including the current key.
        partitions.add(new ClusterByPartition(startKey, key));
        startKey = key;
        partitionCount = keyWeight;
      } else {
        // Add to existing partition.
        partitionCount = partitionCountAfterKey;
      }
    }

    // Add the last partition.
    partitions.add(new ClusterByPartition(startKey, null));

    return new ClusterByPartitions(partitions);
  }

  @JsonProperty("keys")
  TreeSet<ClusterByKey> getRetainedKeys()
  {
    return retainedKeys;
  }

  @JsonProperty("maxKeys")
  int getMaxKeys()
  {
    return maxKeys;
  }

  @JsonProperty("spaceReductionFactor")
  int getSpaceReductionFactor()
  {
    return spaceReductionFactor;
  }

  /**
   * Returns whether a key would be selected by the current spaceReductionFactor.
   */
  private boolean isKeySelected(final ClusterByKey key)
  {
    return spaceReductionFactor == 0 || Integer.numberOfTrailingZeros(key.hashCode()) >= spaceReductionFactor;
  }

  private void increaseSpaceReductionFactor()
  {
    if (spaceReductionFactor == Integer.SIZE) {
      // This is the biggest possible spaceReductionFactor.
      // TODO(gianm): sensible fault
      throw new ISE("Too many keys (estimated = %,d, retained = %,d)", estimatedCount(), retainedKeys.size());
    }

    if (retainedKeys.isEmpty()) {
      return;
    }

    spaceReductionFactor++;

    final Iterator<ClusterByKey> iterator = retainedKeys.iterator();

    // Never remove the first key. Skip it.
    if (iterator.hasNext()) {
      iterator.next();
    }

    while (iterator.hasNext()) {
      final ClusterByKey key = iterator.next();

      if (!isKeySelected(key)) {
        iterator.remove();
      }
    }
  }
}
