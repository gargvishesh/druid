/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ClusterByStatisticsCollectorImpl<CollectorType extends KeyCollector<CollectorType>>
    implements ClusterByStatisticsCollector
{
  // TODO(gianm): 10, 1.05 chosen for no particular reason other than they seemed likely to work.
  private static final int MAX_COUNT_MAX_ITERATIONS = 10;
  private static final double MAX_COUNT_ITERATION_GROWTH_FACTOR = 1.05;

  private final ClusterBy clusterBy;
  private final KeyCollectorFactory<CollectorType> keyCollectorFactory;
  private final SortedMap<ClusterByKey, BucketHolder> buckets;

  // TODO(gianm): this is only really here to make sure we don't generate DimensionRangeShardSpec for multi-value dims.
  //   if we could support that in DimensionRangeShardSpec, we could get rid of this.
  private final boolean[] hasMultipleValues;

  // TODO(gianm): max size is better than max keys. what if some of the keys are really big?
  private final int maxRetainedKeys;
  private final int maxBuckets;
  private int totalRetainedKeys;

  private ClusterByStatisticsCollectorImpl(
      final ClusterBy clusterBy,
      final Comparator<ClusterByKey> comparator,
      final KeyCollectorFactory<CollectorType> keyCollectorFactory,
      final int maxRetainedKeys,
      final int maxBuckets
  )
  {
    this.clusterBy = clusterBy;
    this.keyCollectorFactory = keyCollectorFactory;
    this.maxRetainedKeys = maxRetainedKeys;
    this.buckets = new TreeMap<>(comparator);
    this.maxBuckets = maxBuckets;
    this.hasMultipleValues = new boolean[clusterBy.getColumns().size()];
  }

  @SuppressWarnings("rawtypes")
  public static ClusterByStatisticsCollector create(
      final ClusterBy clusterBy,
      final RowSignature signature,
      final int maxRetainedKeys,
      final int maxBuckets,
      final boolean aggregate
  )
  {
    final Comparator<ClusterByKey> comparator = clusterBy.keyComparator(signature);
    final KeyCollectorFactory<?> keyCollectorFactory = KeyCollectors.makeFactory(clusterBy, signature, aggregate);

    //noinspection unchecked
    return new ClusterByStatisticsCollectorImpl(
        clusterBy,
        comparator,
        keyCollectorFactory,
        maxRetainedKeys,
        maxBuckets
    );
  }

  @Override
  public ClusterBy getClusterBy()
  {
    return clusterBy;
  }

  @Override
  public ClusterByStatisticsCollector add(final ClusterByKey key)
  {
    // TODO(gianm): hack alert: see note for hasMultipleValues
    for (int i = 0; i < clusterBy.getColumns().size(); i++) {
      hasMultipleValues[i] |= key.get(i) instanceof List;
    }

    final BucketHolder bucketHolder = getOrCreateBucketHolder(key.trim(clusterBy.getBucketByCount()));

    bucketHolder.keyCollector.add(key);

    totalRetainedKeys += bucketHolder.updateRetainedKeys();
    if (totalRetainedKeys > maxRetainedKeys) {
      downSample();
    }

    assertTotalRetainedKeysIsCorrect();
    return this;
  }

  @Override
  public ClusterByStatisticsCollector addAll(final ClusterByStatisticsCollector other)
  {
    if (other instanceof ClusterByStatisticsCollectorImpl) {
      //noinspection unchecked
      ClusterByStatisticsCollectorImpl<CollectorType> that = (ClusterByStatisticsCollectorImpl<CollectorType>) other;

      // Add all key collectors from the other collector.
      for (Map.Entry<ClusterByKey, BucketHolder> otherBucketEntry : that.buckets.entrySet()) {
        final BucketHolder bucketHolder = getOrCreateBucketHolder(otherBucketEntry.getKey());
        bucketHolder.keyCollector.addAll(otherBucketEntry.getValue().keyCollector);

        totalRetainedKeys += bucketHolder.updateRetainedKeys();
        if (totalRetainedKeys > maxRetainedKeys) {
          downSample();
        }
      }

      // Update hasMultipleValues.
      // TODO(gianm): hack alert: see note for hasMultipleValues
      for (int i = 0; i < clusterBy.getColumns().size(); i++) {
        hasMultipleValues[i] |= that.hasMultipleValues[i];
      }
    } else {
      addAll(other.snapshot());
    }

    assertTotalRetainedKeysIsCorrect();
    return this;
  }

  @Override
  public ClusterByStatisticsCollector addAll(final ClusterByStatisticsSnapshot snapshot)
  {
    // Add all key collectors from the other collector.
    for (ClusterByStatisticsSnapshot.Bucket otherBucket : snapshot.getBuckets()) {
      final CollectorType otherKeyCollector = keyCollectorFactory.fromSnapshot(otherBucket.getKeyCollectorSnapshot());
      final BucketHolder bucketHolder = getOrCreateBucketHolder(otherBucket.getBucketKey());
      bucketHolder.keyCollector.addAll(otherKeyCollector);

      totalRetainedKeys += bucketHolder.updateRetainedKeys();
      if (totalRetainedKeys > maxRetainedKeys) {
        downSample();
      }
    }

    // Update hasMultipleValues.
    // TODO(gianm): hack alert: see note for hasMultipleValues
    for (int keyPosition : snapshot.getHasMultipleValues()) {
      hasMultipleValues[keyPosition] = true;
    }

    assertTotalRetainedKeysIsCorrect();
    return this;
  }

  @Override
  public long estimatedCount()
  {
    long count = 0L;
    for (final BucketHolder bucketHolder : buckets.values()) {
      count += bucketHolder.keyCollector.estimatedCount();
    }
    return count;
  }

  @Override
  public boolean hasMultipleValues(final int keyPosition)
  {
    if (keyPosition < 0 || keyPosition >= clusterBy.getColumns().size()) {
      throw new IAE("Invalid keyPosition [%d]", keyPosition);
    }

    return hasMultipleValues[keyPosition];
  }

  @Override
  public ClusterByStatisticsCollector clear()
  {
    buckets.clear();
    totalRetainedKeys = 0;

    assertTotalRetainedKeysIsCorrect();
    return this;
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetSize(final long targetPartitionSize)
  {
    if (buckets.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final List<ClusterByPartition> partitions = new ArrayList<>();

    for (final BucketHolder bucket : buckets.values()) {
      final List<ClusterByPartition> bucketPartitions =
          bucket.keyCollector.generatePartitionsWithTargetSize(targetPartitionSize).ranges();

      if (!partitions.isEmpty() && !bucketPartitions.isEmpty()) {
        // Stitch up final partition of previous bucket to match the first partition of this bucket.
        partitions.set(
            partitions.size() - 1,
            new ClusterByPartition(
                partitions.get(partitions.size() - 1).getStart(),
                bucketPartitions.get(0).getStart()
            )
        );
      }

      partitions.addAll(bucketPartitions);
    }

    final ClusterByPartitions retVal = new ClusterByPartitions(partitions);

    if (!retVal.allAbutting()) {
      // It's a bug if this happens.
      throw new ISE("Partitions are not all abutting");
    }

    return retVal;
  }

  @Override
  public ClusterByPartitions generatePartitionsWithMaxCount(final int maxNumPartitions)
  {
    if (maxNumPartitions < 1) {
      throw new IAE("Must have at least one partition");
    } else if (buckets.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    } else if (maxNumPartitions == 1 && clusterBy.getBucketByCount() == 0) {
      return new ClusterByPartitions(
          Collections.singletonList(
              new ClusterByPartition(
                  buckets.get(buckets.firstKey()).keyCollector.minKey(),
                  null
              )
          )
      );
    }

    long totalCount = 0;

    for (final BucketHolder bucketHolder : buckets.values()) {
      totalCount += bucketHolder.keyCollector.estimatedCount();
    }

    // Gradually increase targetPartitionSize until we get the right number of partitions.
    ClusterByPartitions ranges;
    long targetPartitionSize = (long) Math.ceil((double) totalCount / maxNumPartitions);
    int iterations = 0;

    do {
      if (iterations++ > MAX_COUNT_MAX_ITERATIONS) {
        // Could happen if there are a large number of partition-by keys, or if there are more buckets than
        // the max partition count.
        throw new ISE("Unable to compute partition ranges");
      }

      ranges = generatePartitionsWithTargetSize(targetPartitionSize);

      targetPartitionSize *= MAX_COUNT_ITERATION_GROWTH_FACTOR;
    } while (ranges.size() > maxNumPartitions);

    return ranges;
  }

  @Override
  public ClusterByStatisticsSnapshot snapshot()
  {
    final List<ClusterByStatisticsSnapshot.Bucket> bucketSnapshots = new ArrayList<>();

    for (final Map.Entry<ClusterByKey, BucketHolder> bucketEntry : buckets.entrySet()) {
      final KeyCollectorSnapshot keyCollectorSnapshot =
          keyCollectorFactory.toSnapshot(bucketEntry.getValue().keyCollector);
      bucketSnapshots.add(new ClusterByStatisticsSnapshot.Bucket(bucketEntry.getKey(), keyCollectorSnapshot));
    }

    // TODO(gianm): hack alert: see note for hasMultipleValues
    final IntSet hasMultipleValuesSet = new IntRBTreeSet();
    for (int i = 0; i < hasMultipleValues.length; i++) {
      if (hasMultipleValues[i]) {
        hasMultipleValuesSet.add(i);
      }
    }

    return new ClusterByStatisticsSnapshot(bucketSnapshots, hasMultipleValuesSet);
  }

  @VisibleForTesting
  List<CollectorType> getKeyCollectors()
  {
    return buckets.values().stream().map(holder -> holder.keyCollector).collect(Collectors.toList());
  }

  private BucketHolder getOrCreateBucketHolder(final ClusterByKey bucketKey)
  {
    final BucketHolder existingHolder = buckets.get(Preconditions.checkNotNull(bucketKey, "bucketKey"));

    if (existingHolder != null) {
      return existingHolder;
    } else if (buckets.size() < maxBuckets) {
      final BucketHolder newHolder = new BucketHolder(keyCollectorFactory.newKeyCollector());
      buckets.put(bucketKey, newHolder);
      return newHolder;
    } else {
      throw new TooManyBucketsException(maxBuckets);
    }
  }

  /**
   * Reduce the number of retained keys by about half.
   */
  private void downSample()
  {
    if (totalRetainedKeys < buckets.size()) {
      // TODO(gianm): proper fault for trying to downsample below bucket count (or make it impossible)
      throw new ISE("Cannot downsample any further");
    }

    int newTotalRetainedKeys = totalRetainedKeys;
    final int targetTotalRetainedKeys = totalRetainedKeys / 2;

    // Iterate in descending order by retainedKeys.
    final List<BucketHolder> sortedHolders = new ArrayList<>(buckets.values());
    sortedHolders.sort(Comparator.comparing((BucketHolder holder) -> holder.retainedKeys).reversed());

    int i = 0;
    while (i < sortedHolders.size() && newTotalRetainedKeys > targetTotalRetainedKeys) {
      final BucketHolder bucketHolder = sortedHolders.get(i);
      // TODO(gianm): fault for "cannot downsample any further"
      bucketHolder.keyCollector.downSample();
      newTotalRetainedKeys += bucketHolder.updateRetainedKeys();

      if (i == sortedHolders.size() - 1 || sortedHolders.get(i + 1).retainedKeys > bucketHolder.retainedKeys) {
        i++;
      }
    }

    totalRetainedKeys = newTotalRetainedKeys;
    assertTotalRetainedKeysIsCorrect();
  }

  private void assertTotalRetainedKeysIsCorrect()
  {
    assert totalRetainedKeys ==
           buckets.values().stream().mapToInt(holder -> holder.keyCollector.estimatedRetainedKeys()).sum();
  }

  private class BucketHolder
  {
    private final CollectorType keyCollector;
    private int retainedKeys;

    public BucketHolder(final CollectorType keyCollector)
    {
      this.keyCollector = keyCollector;
      this.retainedKeys = keyCollector.estimatedRetainedKeys();
    }

    public int updateRetainedKeys()
    {
      final int newRetainedKeys = keyCollector.estimatedRetainedKeys();
      final int difference = newRetainedKeys - retainedKeys;
      retainedKeys = newRetainedKeys;
      return difference;
    }
  }
}
