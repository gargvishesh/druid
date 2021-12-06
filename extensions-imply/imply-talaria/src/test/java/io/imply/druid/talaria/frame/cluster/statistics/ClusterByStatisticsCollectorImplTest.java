/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class ClusterByStatisticsCollectorImplTest
{
  private static final double PARTITION_SIZE_LEEWAY = 0.3;

  private static final RowSignature SIGNATURE =
      RowSignature.builder().add("x", ColumnType.LONG).add("y", ColumnType.LONG).build();

  private static final ClusterBy CLUSTER_BY_X = new ClusterBy(
      ImmutableList.of(new ClusterByColumn("x", false)),
      0
  );

  private static final ClusterBy CLUSTER_BY_XY_BUCKET_BY_X = new ClusterBy(
      ImmutableList.of(new ClusterByColumn("x", false), new ClusterByColumn("y", false)),
      1
  );

  private static final int MAX_KEYS = 10000;
  private static final int MAX_BUCKETS = 10000;

  @Test
  public void test_clusterByX_unique()
  {
    final long numRows = 1_000_000;
    final boolean aggregate = false;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final Iterable<ClusterByKey> keys = () ->
        LongStream.range(0, numRows).mapToObj(ClusterByKey::of).iterator();

    final Object2IntSortedMap<ClusterByKey> sortedKeyCounts =
        computeSortedKeyCounts(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: tracked bucket count", testName), 1, trackedBuckets(collector));
          Assert.assertEquals(StringUtils.format("%s: tracked row count", testName), numRows, trackedRows(collector));

          for (int targetPartitionSize : new int[]{51111, 65432, (int) numRows + 10}) {
            verifyPartitionsWithTargetSize(
                StringUtils.format("%s: generatePartitionsWithTargetSize(%d)", testName, targetPartitionSize),
                collector,
                targetPartitionSize,
                sortedKeyCounts,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyCounts,
                aggregate
            );
          }
        }
    );
  }

  @Test
  public void test_clusterByX_everyKeyAppearsTwice()
  {
    final long numRows = 1_000_000;
    final boolean aggregate = false;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final List<ClusterByKey> keys = new ArrayList<>();

    for (int i = 0; i < numRows / 2; i++) {
      keys.add(ClusterByKey.of((long) i));
      keys.add(ClusterByKey.of((long) i));
    }

    final Object2IntSortedMap<ClusterByKey> sortedKeyCounts =
        computeSortedKeyCounts(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: tracked bucket count", testName), 1, trackedBuckets(collector));
          Assert.assertEquals(StringUtils.format("%s: tracked row count", testName), numRows, trackedRows(collector));

          for (int targetPartitionSize : new int[]{51111, 65432, (int) numRows + 10}) {
            verifyPartitionsWithTargetSize(
                StringUtils.format("%s: generatePartitionsWithTargetSize(%d)", testName, targetPartitionSize),
                collector,
                targetPartitionSize,
                sortedKeyCounts,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyCounts,
                aggregate
            );
          }
        }
    );
  }

  @Test
  public void test_clusterByX_everyKeyAppearsTwice_withAggregation()
  {
    final long numRows = 1_000_000;
    final boolean aggregate = true;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final List<ClusterByKey> keys = new ArrayList<>();
    final int duplicationFactor = 2;

    for (int i = 0; i < numRows / duplicationFactor; i++) {
      for (int j = 0; j < duplicationFactor; j++) {
        keys.add(ClusterByKey.of((long) i));
      }
    }

    final Object2IntSortedMap<ClusterByKey> sortedKeyCounts =
        computeSortedKeyCounts(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: tracked bucket count", testName), 1, trackedBuckets(collector));

          final double expectedNumRows = (double) numRows / duplicationFactor;
          Assert.assertEquals(
              StringUtils.format("%s: tracked row count", testName),
              expectedNumRows,
              (double) trackedRows(collector),
              expectedNumRows * .05 // Acceptable estimation error
          );

          for (int targetPartitionSize : new int[]{51111, 65432, (int) numRows + 10}) {
            verifyPartitionsWithTargetSize(
                StringUtils.format("%s: generatePartitionsWithTargetSize(%d)", testName, targetPartitionSize),
                collector,
                targetPartitionSize,
                sortedKeyCounts,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 5, 25}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyCounts,
                aggregate
            );
          }
        }
    );
  }

  @Test
  public void test_clusterByXYbucketByX_threeX_uniqueY()
  {
    final int numBuckets = 3;
    final boolean aggregate = false;
    final long numRows = 1_000_000;
    final ClusterBy clusterBy = CLUSTER_BY_XY_BUCKET_BY_X;
    final List<ClusterByKey> keys = new ArrayList<>((int) numRows);

    for (int i = 0; i < numRows; i++) {
      final Object[] key = new Object[2];
      key[0] = (long) (i % numBuckets);
      key[1] = (long) i;
      keys.add(ClusterByKey.of(key));
    }

    final Object2IntSortedMap<ClusterByKey> sortedKeyCounts =
        computeSortedKeyCounts(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: bucket count", testName), numBuckets, trackedBuckets(collector));
          Assert.assertEquals(StringUtils.format("%s: row count", testName), numRows, trackedRows(collector));

          for (int targetPartitionSize : new int[]{17001, 23007}) {
            verifyPartitionsWithTargetSize(
                StringUtils.format("%s: generatePartitionsWithTargetSize(%d)", testName, targetPartitionSize),
                collector,
                targetPartitionSize,
                sortedKeyCounts,
                aggregate
            );
          }

          // TODO(gianm): Tests for maxPartitionCount = 1, 2; should fail because there are too many buckets
          for (int maxPartitionCount : new int[]{3, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyCounts,
                aggregate
            );
          }
        }
    );
  }

  private void doTest(
      final ClusterBy clusterBy,
      final boolean aggregate,
      final Iterable<ClusterByKey> keys,
      final BiConsumer<String, ClusterByStatisticsCollectorImpl<?>> testFn
  )
  {
    final Comparator<ClusterByKey> comparator = clusterBy.keyComparator(SIGNATURE);

    // Load into single collector, sorted order.
    final ClusterByStatisticsCollectorImpl<?> sortedCollector = makeCollector(clusterBy, aggregate);
    final List<ClusterByKey> sortedKeys = Lists.newArrayList(keys);
    sortedKeys.sort(comparator);
    sortedKeys.forEach(sortedCollector::add);
    testFn.accept("single collector, sorted order", sortedCollector);

    // Load into single collector, reverse sorted order.
    final ClusterByStatisticsCollectorImpl<?> reverseSortedCollector = makeCollector(clusterBy, aggregate);
    final List<ClusterByKey> reverseSortedKeys = Lists.newArrayList(keys);
    reverseSortedKeys.sort(comparator.reversed());
    reverseSortedKeys.forEach(reverseSortedCollector::add);
    testFn.accept("single collector, reverse sorted order", reverseSortedCollector);

    // Randomized load into single collector.
    final ClusterByStatisticsCollectorImpl<?> randomizedCollector = makeCollector(clusterBy, aggregate);
    final List<ClusterByKey> randomizedKeys = Lists.newArrayList(keys);
    Collections.shuffle(randomizedKeys, new Random(7 /* Consistent seed from run to run */));
    randomizedKeys.forEach(randomizedCollector::add);
    testFn.accept("single collector, random order", randomizedCollector);

    // Split randomized load into three collectors of the same size, followed by merge.
    final List<ClusterByStatisticsCollectorImpl<?>> threeEqualSizedCollectors = new ArrayList<>();
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));

    final Iterator<ClusterByKey> iterator1 = randomizedKeys.iterator();
    for (int i = 0; iterator1.hasNext(); i++) {
      final ClusterByKey key = iterator1.next();
      threeEqualSizedCollectors.get(i % threeEqualSizedCollectors.size()).add(key);
    }

    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(1)); // Regular add
    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(2).snapshot()); // Snapshot add

    testFn.accept("three merged collectors, equal sizes", threeEqualSizedCollectors.get(0));

    // Split randomized load into three collectors of different sizes, followed by merge.
    final List<ClusterByStatisticsCollectorImpl<?>> threeDifferentlySizedCollectors = new ArrayList<>();
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));

    final Iterator<ClusterByKey> iterator2 = randomizedKeys.iterator();
    for (int i = 0; iterator2.hasNext(); i++) {
      final ClusterByKey key = iterator2.next();

      if (i % 100 < 2) {
        // 2% of space
        threeDifferentlySizedCollectors.get(0).add(key);
      } else if (i % 100 < 20) {
        // 18% of space
        threeDifferentlySizedCollectors.get(1).add(key);
      } else {
        // 80% of space
        threeDifferentlySizedCollectors.get(2).add(key);
      }
    }

    threeDifferentlySizedCollectors.get(0).addAll(threeDifferentlySizedCollectors.get(1)); // Big into small
    threeDifferentlySizedCollectors.get(2).addAll(threeDifferentlySizedCollectors.get(0)); // Small into big

    testFn.accept("three merged collectors, different sizes", threeDifferentlySizedCollectors.get(2));
  }

  private ClusterByStatisticsCollectorImpl<?> makeCollector(final ClusterBy clusterBy, final boolean aggregate)
  {
    return (ClusterByStatisticsCollectorImpl<?>)
        ClusterByStatisticsCollectorImpl.create(clusterBy, SIGNATURE, MAX_KEYS, MAX_BUCKETS, aggregate);
  }

  private static void verifyPartitions(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts,
      final boolean aggregate,
      final long expectedPartitionSize
  )
  {
    final int expectedNumberOfBuckets =
        sortedKeyCounts.keySet()
                       .stream()
                       .map(key -> key.trim(clusterBy.getBucketByCount()))
                       .collect(Collectors.toSet())
                       .size();

    verifyPartitionsCoverKeySpace(testName, partitions, sortedKeyCounts, clusterBy.keyComparator(SIGNATURE));
    verifyNumberOfBuckets(testName, clusterBy, partitions, expectedNumberOfBuckets);
    verifyPartitionsRespectBucketBoundaries(testName, clusterBy, partitions, sortedKeyCounts);
    verifyPartitionSizes(testName, clusterBy, partitions, sortedKeyCounts, aggregate, expectedPartitionSize);
  }

  private static void verifyPartitionsWithTargetSize(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final int targetPartitionSize,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts,
      final boolean aggregate
  )
  {
    verifyPartitions(
        testName,
        collector.getClusterBy(),
        collector.generatePartitionsWithTargetSize(targetPartitionSize),
        sortedKeyCounts,
        aggregate,
        targetPartitionSize
    );
  }

  private static void verifyPartitionsWithMaxCount(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final int maxPartitionCount,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts,
      final boolean aggregate
  )
  {
    final ClusterByPartitions partitions = collector.generatePartitionsWithMaxCount(maxPartitionCount);

    verifyPartitions(
        testName,
        collector.getClusterBy(),
        partitions,
        sortedKeyCounts,
        aggregate,
        totalRows(sortedKeyCounts, aggregate) / maxPartitionCount
    );

    MatcherAssert.assertThat(
        StringUtils.format("%s: number of partitions ≤ max", testName),
        partitions.size(),
        Matchers.lessThanOrEqualTo(maxPartitionCount)
    );
  }

  private static void verifyNumberOfBuckets(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final int expectedNumberOfBuckets
  )
  {
    Assert.assertEquals(
        StringUtils.format("%s: number of buckets", testName),
        expectedNumberOfBuckets,
        partitions.ranges()
                  .stream()
                  .map(partition -> partition.getStart().trim(clusterBy.getBucketByCount()))
                  .distinct()
                  .count()
    );
  }

  /**
   * Verify that:
   *
   * - Partitions are all abutting
   * - The start of the first partition matches the minimum key (if there are keys)
   * - The end of the last partition is null
   * - Each partition's end is after its start
   */
  private static void verifyPartitionsCoverKeySpace(
      final String testName,
      final ClusterByPartitions partitions,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts,
      final Comparator<ClusterByKey> comparator
  )
  {
    Assert.assertTrue(StringUtils.format("%s: partitions abutting", testName), partitions.allAbutting());

    final ClusterByKey minKey = sortedKeyCounts.firstKey();
    final List<ClusterByPartition> ranges = partitions.ranges();

    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);

      // Check expected nullness of the start key.
      if (i == 0) {
        Assert.assertEquals(
            StringUtils.format("%s: partition %d: start is null", testName, i),
            minKey,
            partition.getStart()
        );
      } else {
        Assert.assertNotNull(
            StringUtils.format("%s: partition %d: start is nonnull", testName, i),
            partition.getStart()
        );
      }

      // Check expected nullness of the end key.
      if (i == ranges.size() - 1) {
        Assert.assertNull(
            StringUtils.format("%s: partition %d (final): end is null", testName, i),
            partition.getEnd()
        );
      } else {
        Assert.assertNotNull(
            StringUtils.format("%s: partition %d: end is nonnull", testName, i),
            partition.getEnd()
        );
      }

      // Check that the ends are all after the starts.
      if (partition.getStart() != null && partition.getEnd() != null) {
        MatcherAssert.assertThat(
            StringUtils.format("%s: partition %d: start compareTo end", testName, i),
            comparator.compare(partition.getStart(), partition.getEnd()),
            Matchers.lessThan(0)
        );
      }
    }
  }

  /**
   * Verify that no partition spans more than one bucket.
   */
  private static void verifyPartitionsRespectBucketBoundaries(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts
  )
  {
    final List<ClusterByPartition> ranges = partitions.ranges();

    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);
      final Object2IntSortedMap<ClusterByKey> partitionKeyCounts = getPartitionKeyCounts(partition, sortedKeyCounts);
      final ClusterByKey firstBucketKey = partitionKeyCounts.firstKey().trim(clusterBy.getBucketByCount());
      final ClusterByKey lastBucketKey = partitionKeyCounts.lastKey().trim(clusterBy.getBucketByCount());

      Assert.assertEquals(
          StringUtils.format("%s: partition %d: first, last bucket key", testName, i),
          firstBucketKey,
          lastBucketKey
      );
    }
  }

  /**
   * Verify that partitions have "reasonable" sizes.
   */
  private static void verifyPartitionSizes(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts,
      final boolean aggregate,
      final long expectedPartitionSize
  )
  {
    final List<ClusterByPartition> ranges = partitions.ranges();

    // Compute actual number of rows per partition.
    final Map<ClusterByKey, Long> rowsPerPartition = new HashMap<>();

    for (final ClusterByPartition partition : partitions) {
      rowsPerPartition.put(
          partition.getStart(),
          totalRows(getPartitionKeyCounts(partition, sortedKeyCounts), aggregate)
      );
    }

    // Compare actual size to desired size.
    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);
      final ClusterByKey bucketKey = partition.getStart().trim(clusterBy.getBucketByCount());
      final long actualNumberOfRows = rowsPerPartition.get(partition.getStart());

      // Reasonable maximum number of rows per partition.
      MatcherAssert.assertThat(
          StringUtils.format("%s: partition #%d: number of rows", testName, i),
          actualNumberOfRows,
          Matchers.lessThan((long) ((1 + PARTITION_SIZE_LEEWAY) * expectedPartitionSize))
      );

      // Reasonable minimum number of rows per partition, for all partitions except the last in a bucket.
      // Our algorithm allows the last partition of each bucketto be extra-small.
      final boolean isLastInBucket =
          i == partitions.size() - 1
          || !partitions.get(i + 1).getStart().trim(clusterBy.getBucketByCount()).equals(bucketKey);

      if (!isLastInBucket) {
        MatcherAssert.assertThat(
            StringUtils.format("%s: partition #%d: number of rows", testName, i),
            actualNumberOfRows,
            Matchers.greaterThanOrEqualTo((long) ((1 - PARTITION_SIZE_LEEWAY) * expectedPartitionSize))
        );
      }
    }
  }

  private static Object2IntSortedMap<ClusterByKey> getPartitionKeyCounts(
      final ClusterByPartition partition,
      final Object2IntSortedMap<ClusterByKey> sortedKeyCounts
  )
  {
    if (partition.getEnd() != null) {
      return sortedKeyCounts.subMap(partition.getStart(), partition.getEnd());
    } else {
      return sortedKeyCounts.tailMap(partition.getStart());
    }
  }

  private static Object2IntSortedMap<ClusterByKey> computeSortedKeyCounts(
      final Iterable<ClusterByKey> keys,
      final Comparator<ClusterByKey> comparator
  )
  {
    final Object2IntSortedMap<ClusterByKey> sortedKeyCounts = new Object2IntAVLTreeMap<>(comparator);
    sortedKeyCounts.defaultReturnValue(0);

    for (final ClusterByKey key : keys) {
      sortedKeyCounts.put(key, sortedKeyCounts.getInt(key) + 1);
    }

    return sortedKeyCounts;
  }

  private static long trackedBuckets(final ClusterByStatisticsCollectorImpl<? extends KeyCollector<?>> collector)
  {
    return collector.getKeyCollectors().size();
  }

  private static long trackedRows(final ClusterByStatisticsCollectorImpl<? extends KeyCollector<?>> collector)
  {
    long count = 0;
    for (final KeyCollector<?> keyCollector : collector.getKeyCollectors()) {
      count += keyCollector.estimatedCount();
    }
    return count;
  }

  private static long totalRows(final Object2IntMap<ClusterByKey> keyCounts, final boolean aggregate)
  {
    if (aggregate) {
      return keyCounts.size();
    } else {
      long count = 0;

      final IntIterator iterator = keyCounts.values().iterator();
      while (iterator.hasNext()) {
        count += iterator.nextInt();
      }

      return count;
    }
  }
}
