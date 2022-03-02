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

public class ClusterByStatisticsCollectorImpl implements ClusterByStatisticsCollector
{
  // Used for generatePartitionsWithMaxCount; see https://github.com/implydata/druid/pull/884 for potentially
  // better implementation.
  private static final int MAX_COUNT_MAX_ITERATIONS = 500;
  private static final double MAX_COUNT_ITERATION_GROWTH_FACTOR = 1.05;

  private final ClusterBy clusterBy;
  private final KeyCollectorFactory<? extends KeyCollector<?>, ? extends KeyCollectorSnapshot> keyCollectorFactory;
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
      final KeyCollectorFactory<?, ?> keyCollectorFactory,
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

    if (maxBuckets > maxRetainedKeys) {
      throw new IAE("maxBuckets[%s] cannot be larger than maxRetainedKeys[%s]", maxBuckets, maxRetainedKeys);
    }
  }

  public static ClusterByStatisticsCollector create(
      final ClusterBy clusterBy,
      final RowSignature signature,
      final int maxRetainedKeys,
      final int maxBuckets,
      final boolean aggregate
  )
  {
    final Comparator<ClusterByKey> comparator = clusterBy.keyComparator(signature);
    final KeyCollectorFactory<?, ?> keyCollectorFactory = KeyCollectors.makeStandardFactory(
        clusterBy,
        signature,
        aggregate
    );

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
  public ClusterByStatisticsCollector add(final ClusterByKey key, final int weight)
  {
    // TODO(gianm): hack alert: see note for hasMultipleValues
    for (int i = 0; i < clusterBy.getColumns().size(); i++) {
      hasMultipleValues[i] |= key.get(i) instanceof List;
    }

    final BucketHolder bucketHolder = getOrCreateBucketHolder(key.trim(clusterBy.getBucketByCount()));

    bucketHolder.keyCollector.add(key, weight);

    totalRetainedKeys += bucketHolder.updateRetainedKeys();
    if (totalRetainedKeys > maxRetainedKeys) {
      downSample();
    }

    return this;
  }

  @Override
  public ClusterByStatisticsCollector addAll(final ClusterByStatisticsCollector other)
  {
    if (other instanceof ClusterByStatisticsCollectorImpl) {
      ClusterByStatisticsCollectorImpl that = (ClusterByStatisticsCollectorImpl) other;

      // Add all key collectors from the other collector.
      for (Map.Entry<ClusterByKey, BucketHolder> otherBucketEntry : that.buckets.entrySet()) {
        final BucketHolder bucketHolder = getOrCreateBucketHolder(otherBucketEntry.getKey());

        //noinspection rawtypes, unchecked
        ((KeyCollector) bucketHolder.keyCollector).addAll(otherBucketEntry.getValue().keyCollector);

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

    return this;
  }

  @Override
  public ClusterByStatisticsCollector addAll(final ClusterByStatisticsSnapshot snapshot)
  {
    // Add all key collectors from the other collector.
    for (ClusterByStatisticsSnapshot.Bucket otherBucket : snapshot.getBuckets()) {
      //noinspection rawtypes, unchecked
      final KeyCollector<?> otherKeyCollector =
          ((KeyCollectorFactory) keyCollectorFactory).fromSnapshot(otherBucket.getKeyCollectorSnapshot());
      final BucketHolder bucketHolder = getOrCreateBucketHolder(otherBucket.getBucketKey());

      //noinspection rawtypes, unchecked
      ((KeyCollector) bucketHolder.keyCollector).addAll(otherKeyCollector);

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

    return this;
  }

  @Override
  public long estimatedTotalWeight()
  {
    long count = 0L;
    for (final BucketHolder bucketHolder : buckets.values()) {
      count += bucketHolder.keyCollector.estimatedTotalWeight();
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
    return this;
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetWeight(final long targetWeight)
  {
    if (targetWeight < 1) {
      throw new IAE("Target weight must be positive");
    }

    assertRetainedKeyCountsAreTrackedCorrectly();

    if (buckets.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final List<ClusterByPartition> partitions = new ArrayList<>();

    for (final BucketHolder bucket : buckets.values()) {
      final List<ClusterByPartition> bucketPartitions =
          bucket.keyCollector.generatePartitionsWithTargetWeight(targetWeight).ranges();

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

    long totalWeight = 0;

    for (final BucketHolder bucketHolder : buckets.values()) {
      totalWeight += bucketHolder.keyCollector.estimatedTotalWeight();
    }

    // Gradually increase targetPartitionSize until we get the right number of partitions.
    ClusterByPartitions ranges;
    long targetPartitionWeight = (long) Math.ceil((double) totalWeight / maxNumPartitions);
    int iterations = 0;

    do {
      if (iterations++ > MAX_COUNT_MAX_ITERATIONS) {
        // Could happen if there are a large number of partition-by keys, or if there are more buckets than
        // the max partition count.
        throw new ISE("Unable to compute partition ranges");
      }

      ranges = generatePartitionsWithTargetWeight(targetPartitionWeight);

      targetPartitionWeight = (long) Math.ceil(targetPartitionWeight * MAX_COUNT_ITERATION_GROWTH_FACTOR);
    } while (ranges.size() > maxNumPartitions);

    return ranges;
  }

  @Override
  public ClusterByStatisticsSnapshot snapshot()
  {
    assertRetainedKeyCountsAreTrackedCorrectly();

    final List<ClusterByStatisticsSnapshot.Bucket> bucketSnapshots = new ArrayList<>();

    for (final Map.Entry<ClusterByKey, BucketHolder> bucketEntry : buckets.entrySet()) {
      //noinspection rawtypes, unchecked
      final KeyCollectorSnapshot keyCollectorSnapshot =
          ((KeyCollectorFactory) keyCollectorFactory).toSnapshot(bucketEntry.getValue().keyCollector);
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
  List<KeyCollector<?>> getKeyCollectors()
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
   * Reduce the number of retained keys by about half, if possible. May reduce by less than that, or keep the
   * number the same, if downsampling is not possible. (For example: downsampling is not possible if all buckets
   * have been downsampled all the way to one key each.)
   */
  private void downSample()
  {
    int newTotalRetainedKeys = totalRetainedKeys;
    final int targetTotalRetainedKeys = totalRetainedKeys / 2;

    final List<BucketHolder> sortedHolders = new ArrayList<>(buckets.size());

    // Only consider holders with more than one retained key. Holders with a single retained key cannot be downsampled.
    for (final BucketHolder holder : buckets.values()) {
      if (holder.retainedKeys > 1) {
        sortedHolders.add(holder);
      }
    }

    // Downsample least-dense buckets first. (They're less likely to need high resolution.)
    sortedHolders.sort(
        Comparator.comparing((BucketHolder holder) ->
                                 (double) holder.keyCollector.estimatedTotalWeight() / holder.retainedKeys)
    );

    int i = 0;
    while (i < sortedHolders.size() && newTotalRetainedKeys > targetTotalRetainedKeys) {
      final BucketHolder bucketHolder = sortedHolders.get(i);

      // Ignore false return, because we wrap all collectors in DelegateOrMinKeyCollector and can be assured that
      // it will downsample all the way to one if needed. Can't do better than that.
      bucketHolder.keyCollector.downSample();
      newTotalRetainedKeys += bucketHolder.updateRetainedKeys();

      if (i == sortedHolders.size() - 1 || sortedHolders.get(i + 1).retainedKeys > bucketHolder.retainedKeys) {
        i++;
      }
    }

    totalRetainedKeys = newTotalRetainedKeys;
  }

  private void assertRetainedKeyCountsAreTrackedCorrectly()
  {
    // Check cached value of retainedKeys in each holder.
    assert buckets.values()
                  .stream()
                  .allMatch(holder -> holder.retainedKeys == holder.keyCollector.estimatedRetainedKeys());

    // Check cached value of totalRetainedKeys.
    assert totalRetainedKeys ==
           buckets.values().stream().mapToInt(holder -> holder.keyCollector.estimatedRetainedKeys()).sum();
  }

  private static class BucketHolder
  {
    private final KeyCollector<?> keyCollector;
    private int retainedKeys;

    public BucketHolder(final KeyCollector<?> keyCollector)
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
