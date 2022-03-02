/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByKeyDeserializerModule;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
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

  // These numbers are roughly 10x lower than authentic production numbers. (See StageDefinition.)
  private static final int MAX_KEYS = 5000;
  private static final int MAX_BUCKETS = 1000;

  @Test
  public void test_clusterByX_unique()
  {
    final long numRows = 1_000_000;
    final boolean aggregate = false;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final Iterable<ClusterByKey> keys = () ->
        LongStream.range(0, numRows).mapToObj(ClusterByKey::of).iterator();

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: tracked bucket count", testName), 1, trackedBuckets(collector));
          Assert.assertEquals(StringUtils.format("%s: tracked row count", testName), numRows, trackedRows(collector));

          for (int targetPartitionWeight : new int[]{51111, 65432, (int) numRows + 10}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyWeights,
                aggregate
            );
          }

          verifySnapshotSerialization(testName, collector, aggregate);
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

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: tracked bucket count", testName), 1, trackedBuckets(collector));
          Assert.assertEquals(StringUtils.format("%s: tracked row count", testName), numRows, trackedRows(collector));

          for (int targetPartitionWeight : new int[]{51111, 65432, (int) numRows + 10}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyWeights,
                aggregate
            );
          }

          verifySnapshotSerialization(testName, collector, aggregate);
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

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

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

          for (int targetPartitionWeight : new int[]{51111, 65432, (int) numRows + 10}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 5, 25}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyWeights,
                aggregate
            );
          }

          verifySnapshotSerialization(testName, collector, aggregate);
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

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: bucket count", testName), numBuckets, trackedBuckets(collector));
          Assert.assertEquals(StringUtils.format("%s: row count", testName), numRows, trackedRows(collector));

          for (int targetPartitionWeight : new int[]{17001, 23007}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 3, 10, 50}) {
            if (maxPartitionCount < numBuckets) {
              final IllegalStateException e = Assert.assertThrows(
                  IllegalStateException.class,
                  () -> collector.generatePartitionsWithMaxCount(maxPartitionCount)
              );

              MatcherAssert.assertThat(
                  e,
                  ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Unable to compute partition ranges"))
              );
            } else {
              verifyPartitionsWithMaxCount(
                  StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                  collector,
                  maxPartitionCount,
                  sortedKeyWeights,
                  aggregate
              );
            }
          }

          verifySnapshotSerialization(testName, collector, aggregate);
        }
    );
  }

  @Test
  public void test_clusterByXYbucketByX_maxX_uniqueY()
  {
    final int numBuckets = MAX_BUCKETS;
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

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: bucket count", testName), numBuckets, trackedBuckets(collector));

          for (int targetPartitionWeight : new int[]{17001, 23007}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 10, numBuckets - 1, numBuckets}) {
            if (maxPartitionCount < numBuckets) {
              // Cannot compute partitions ranges when maxPartitionCount < numBuckets, because there must be at
              // least one partition per bucket.
              final IllegalStateException e = Assert.assertThrows(
                  IllegalStateException.class,
                  () -> verifyPartitionsWithMaxCount(
                      StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                      collector,
                      maxPartitionCount,
                      sortedKeyWeights,
                      aggregate
                  )
              );

              MatcherAssert.assertThat(
                  e,
                  ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Unable to compute partition ranges"))
              );
            } else {
              verifyPartitionsWithMaxCount(
                  StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                  collector,
                  maxPartitionCount,
                  sortedKeyWeights,
                  aggregate
              );
            }
          }

          verifySnapshotSerialization(testName, collector, aggregate);
        }
    );
  }

  @Test
  public void test_clusterByXYbucketByX_maxX_lowCardinalityY_withAggregation()
  {
    final int numBuckets = MAX_BUCKETS;
    final boolean aggregate = true;
    final long numRows = 1_000_000;
    final ClusterBy clusterBy = CLUSTER_BY_XY_BUCKET_BY_X;
    final List<ClusterByKey> keys = new ArrayList<>((int) numRows);

    for (int i = 0; i < numRows; i++) {
      final Object[] key = new Object[2];
      key[0] = (long) (i % numBuckets);
      key[1] = (long) (i % 5); // Only five different Ys
      keys.add(ClusterByKey.of(key));
    }

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assert.assertEquals(StringUtils.format("%s: bucket count", testName), numBuckets, trackedBuckets(collector));

          // trackedRows will equal numBuckets, because the collectors have been downsampled so much
          Assert.assertEquals(StringUtils.format("%s: row count", testName), numBuckets, trackedRows(collector));

          for (int targetPartitionWeight : new int[]{17001, 23007}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 10, numBuckets, numBuckets + 1}) {
            if (maxPartitionCount < numBuckets) {
              final IllegalStateException e = Assert.assertThrows(
                  IllegalStateException.class,
                  () -> collector.generatePartitionsWithMaxCount(maxPartitionCount)
              );

              MatcherAssert.assertThat(
                  e,
                  ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Unable to compute partition ranges"))
              );
            } else {
              verifyPartitionsWithMaxCount(
                  StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                  collector,
                  maxPartitionCount,
                  sortedKeyWeights,
                  aggregate
              );
            }
          }

          verifySnapshotSerialization(testName, collector, aggregate);
        }
    );
  }

  private void doTest(
      final ClusterBy clusterBy,
      final boolean aggregate,
      final Iterable<ClusterByKey> keys,
      final BiConsumer<String, ClusterByStatisticsCollectorImpl> testFn
  )
  {
    final Comparator<ClusterByKey> comparator = clusterBy.keyComparator(SIGNATURE);

    // Load into single collector, sorted order.
    final ClusterByStatisticsCollectorImpl sortedCollector = makeCollector(clusterBy, aggregate);
    final List<ClusterByKey> sortedKeys = Lists.newArrayList(keys);
    sortedKeys.sort(comparator);
    sortedKeys.forEach(k -> sortedCollector.add(k, 1));
    testFn.accept("single collector, sorted order", sortedCollector);

    // Load into single collector, reverse sorted order.
    final ClusterByStatisticsCollectorImpl reverseSortedCollector = makeCollector(clusterBy, aggregate);
    final List<ClusterByKey> reverseSortedKeys = Lists.newArrayList(keys);
    reverseSortedKeys.sort(comparator.reversed());
    reverseSortedKeys.forEach(k -> reverseSortedCollector.add(k, 1));
    testFn.accept("single collector, reverse sorted order", reverseSortedCollector);

    // Randomized load into single collector.
    final ClusterByStatisticsCollectorImpl randomizedCollector = makeCollector(clusterBy, aggregate);
    final List<ClusterByKey> randomizedKeys = Lists.newArrayList(keys);
    Collections.shuffle(randomizedKeys, new Random(7 /* Consistent seed from run to run */));
    randomizedKeys.forEach(k -> randomizedCollector.add(k, 1));
    testFn.accept("single collector, random order", randomizedCollector);

    // Split randomized load into three collectors of the same size, followed by merge.
    final List<ClusterByStatisticsCollectorImpl> threeEqualSizedCollectors = new ArrayList<>();
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));

    final Iterator<ClusterByKey> iterator1 = randomizedKeys.iterator();
    for (int i = 0; iterator1.hasNext(); i++) {
      final ClusterByKey key = iterator1.next();
      threeEqualSizedCollectors.get(i % threeEqualSizedCollectors.size()).add(key, 1);
    }

    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(1)); // Regular add
    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(2).snapshot()); // Snapshot add

    testFn.accept("three merged collectors, equal sizes", threeEqualSizedCollectors.get(0));

    // Split randomized load into three collectors of different sizes, followed by merge.
    final List<ClusterByStatisticsCollectorImpl> threeDifferentlySizedCollectors = new ArrayList<>();
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));

    final Iterator<ClusterByKey> iterator2 = randomizedKeys.iterator();
    for (int i = 0; iterator2.hasNext(); i++) {
      final ClusterByKey key = iterator2.next();

      if (i % 100 < 2) {
        // 2% of space
        threeDifferentlySizedCollectors.get(0).add(key, 1);
      } else if (i % 100 < 20) {
        // 18% of space
        threeDifferentlySizedCollectors.get(1).add(key, 1);
      } else {
        // 80% of space
        threeDifferentlySizedCollectors.get(2).add(key, 1);
      }
    }

    threeDifferentlySizedCollectors.get(0).addAll(threeDifferentlySizedCollectors.get(1)); // Big into small
    threeDifferentlySizedCollectors.get(2).addAll(threeDifferentlySizedCollectors.get(0)); // Small into big

    testFn.accept("three merged collectors, different sizes", threeDifferentlySizedCollectors.get(2));
  }

  private ClusterByStatisticsCollectorImpl makeCollector(final ClusterBy clusterBy, final boolean aggregate)
  {
    return (ClusterByStatisticsCollectorImpl)
        ClusterByStatisticsCollectorImpl.create(clusterBy, SIGNATURE, MAX_KEYS, MAX_BUCKETS, aggregate);
  }

  private static void verifyPartitions(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate,
      final long expectedPartitionSize
  )
  {
    final int expectedNumberOfBuckets =
        sortedKeyWeights.keySet()
                        .stream()
                        .map(key -> key.trim(clusterBy.getBucketByCount()))
                        .collect(Collectors.toSet())
                        .size();

    verifyNumberOfBuckets(testName, clusterBy, partitions, expectedNumberOfBuckets);
    verifyPartitionsRespectBucketBoundaries(testName, clusterBy, partitions, sortedKeyWeights);
    verifyPartitionsCoverKeySpace(
        testName,
        partitions,
        sortedKeyWeights.firstKey(),
        clusterBy.keyComparator(SIGNATURE)
    );
    verifyPartitionWeights(testName, clusterBy, partitions, sortedKeyWeights, aggregate, expectedPartitionSize);
  }

  private static void verifyPartitionsWithTargetWeight(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final int targetPartitionWeight,
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate
  )
  {
    verifyPartitions(
        testName,
        collector.getClusterBy(),
        collector.generatePartitionsWithTargetWeight(targetPartitionWeight),
        sortedKeyWeights,
        aggregate,
        targetPartitionWeight
    );
  }

  private static void verifyPartitionsWithMaxCount(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final int maxPartitionCount,
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate
  )
  {
    final ClusterByPartitions partitions = collector.generatePartitionsWithMaxCount(maxPartitionCount);

    verifyPartitions(
        testName,
        collector.getClusterBy(),
        partitions,
        sortedKeyWeights,
        aggregate,
        LongMath.divide(
            totalWeight(sortedKeyWeights, new ClusterByPartition(null, null), aggregate),
            maxPartitionCount,
            RoundingMode.CEILING
        )
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
  static void verifyPartitionsCoverKeySpace(
      final String testName,
      final ClusterByPartitions partitions,
      final ClusterByKey expectedMinKey,
      final Comparator<ClusterByKey> comparator
  )
  {
    Assert.assertTrue(StringUtils.format("%s: partitions abutting", testName), partitions.allAbutting());

    final List<ClusterByPartition> ranges = partitions.ranges();

    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);

      // Check expected nullness of the start key.
      if (i == 0) {
        Assert.assertEquals(
            StringUtils.format("%s: partition %d: start is min key", testName, i),
            expectedMinKey,
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
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights
  )
  {
    final List<ClusterByPartition> ranges = partitions.ranges();

    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);
      final ClusterByKey firstBucketKey = partition.getStart().trim(clusterBy.getBucketByCount());
      final ClusterByKey lastBucketKey =
          (partition.getEnd() == null
           ? sortedKeyWeights.lastKey()
           : sortedKeyWeights.subMap(partition.getStart(), true, partition.getEnd(), false).lastKey())
              .trim(clusterBy.getBucketByCount());

      Assert.assertEquals(
          StringUtils.format("%s: partition %d: first, last bucket key are equal", testName, i),
          firstBucketKey,
          lastBucketKey
      );
    }
  }

  /**
   * Verify that partitions have "reasonable" sizes.
   */
  static void verifyPartitionWeights(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights,
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
          totalWeight(sortedKeyWeights, partition, aggregate)
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
          Matchers.lessThanOrEqualTo((long) ((1 + PARTITION_SIZE_LEEWAY) * expectedPartitionSize))
      );

      // Reasonable minimum number of rows per partition, for all partitions except the last in a bucket.
      // Our algorithm allows the last partition of each bucket to be extra-small.
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

  static NavigableMap<ClusterByKey, List<Integer>> computeSortedKeyWeightsFromWeightedKeys(
      final Iterable<Pair<ClusterByKey, Integer>> keys,
      final Comparator<ClusterByKey> comparator
  )
  {
    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights = new TreeMap<>(comparator);

    for (final Pair<ClusterByKey, Integer> key : keys) {
      sortedKeyWeights.computeIfAbsent(key.lhs, k -> new ArrayList<>()).add(key.rhs);
    }

    return sortedKeyWeights;
  }

  static NavigableMap<ClusterByKey, List<Integer>> computeSortedKeyWeightsFromUnweightedKeys(
      final Iterable<ClusterByKey> keys,
      final Comparator<ClusterByKey> comparator
  )
  {
    return computeSortedKeyWeightsFromWeightedKeys(
        Iterables.transform(keys, key -> Pair.of(key, 1)),
        comparator
    );
  }

  static long totalWeight(
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights,
      final ClusterByPartition partition,
      final boolean aggregate
  )
  {
    final NavigableMap<ClusterByKey, List<Integer>> partitionWeights =
        sortedKeyWeights.subMap(
            partition.getStart() == null ? sortedKeyWeights.firstKey() : partition.getStart(),
            true,
            partition.getEnd() == null ? sortedKeyWeights.lastKey() : partition.getEnd(),
            partition.getEnd() == null
        );

    long retVal = 0;

    for (final Collection<Integer> weights : partitionWeights.values()) {
      if (aggregate) {
        retVal += Collections.max(weights);
      } else {
        for (int w : weights) {
          retVal += w;
        }
      }
    }

    return retVal;
  }

  private static long trackedBuckets(final ClusterByStatisticsCollectorImpl collector)
  {
    return collector.getKeyCollectors().size();
  }

  private static long trackedRows(final ClusterByStatisticsCollectorImpl collector)
  {
    long count = 0;
    for (final KeyCollector<?> keyCollector : collector.getKeyCollectors()) {
      count += keyCollector.estimatedTotalWeight();
    }
    return count;
  }

  private static void verifySnapshotSerialization(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final boolean aggregate
  )
  {
    try {
      final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
      jsonMapper.registerModule(new ClusterByKeyDeserializerModule(SIGNATURE, collector.getClusterBy()));
      jsonMapper.registerModule(
          new KeyCollectorSnapshotDeserializerModule(
              KeyCollectors.makeStandardFactory(
                  collector.getClusterBy(),
                  SIGNATURE,
                  aggregate
              )
          )
      );

      final ClusterByStatisticsSnapshot snapshot = collector.snapshot();
      final ClusterByStatisticsSnapshot snapshot2 = jsonMapper.readValue(
          jsonMapper.writeValueAsString(snapshot),
          ClusterByStatisticsSnapshot.class
      );

      Assert.assertEquals(StringUtils.format("%s: snapshot is serializable", testName), snapshot, snapshot2);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
