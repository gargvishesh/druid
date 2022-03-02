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
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class QuantilesSketchKeyCollectorTest
{
  private final ClusterBy clusterBy = new ClusterBy(ImmutableList.of(new ClusterByColumn("x", false)), 0);
  private final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
  private final Comparator<ClusterByKey> comparator = clusterBy.keyComparator(signature);
  private final int numKeys = 500_000;

  @Test
  public void test_empty()
  {
    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature),
        Collections.emptyList(),
        comparator,
        (testName, collector) -> {
          Assert.assertTrue(collector.isEmpty());
          Assert.assertThrows(NoSuchElementException.class, collector::minKey);
          Assert.assertEquals(testName, 0, collector.estimatedTotalWeight());
          Assert.assertEquals(
              ClusterByPartitions.oneUniversalPartition(),
              collector.generatePartitionsWithTargetWeight(1000)
          );
        }
    );
  }

  @Test
  public void test_sequentialKeys_unweighted()
  {
    final List<Pair<ClusterByKey, Integer>> keyWeights = KeyCollectorTestUtils.sequentialKeys(numKeys);

    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(testName, numKeys, collector.estimatedTotalWeight());
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_unweighted()
  {
    final List<Pair<ClusterByKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);
    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(testName, numKeys, collector.estimatedTotalWeight());
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_unweighted_downSampledToSmallestSize()
  {
    final List<Pair<ClusterByKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);
    final ClusterByKey finalMinKey =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator).firstKey();

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          while (collector.downSample()) {
            // Intentionally empty loop body.
          }

          Assert.assertEquals(testName, 2, collector.getSketch().getK());
          Assert.assertEquals(testName, 22, collector.estimatedRetainedKeys());

          // Don't use verifyCollector, since this collector is downsampled so aggressively that it can't possibly
          // hope to pass those tests. Grade on a curve.
          final ClusterByPartitions partitions = collector.generatePartitionsWithTargetWeight(10_000);
          ClusterByStatisticsCollectorImplTest.verifyPartitionsCoverKeySpace(
              testName,
              partitions,
              finalMinKey,
              comparator
          );
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_barbellWeighted()
  {
    final List<Pair<ClusterByKey, Integer>> keyWeights =
        KeyCollectorTestUtils.uniformRandomBarbellWeightedKeys(numKeys);
    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(
              testName,
              ClusterByStatisticsCollectorImplTest.totalWeight(
                  sortedKeyWeights,
                  new ClusterByPartition(null, null),
                  false
              ),
              collector.estimatedTotalWeight()
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_inverseBarbellWeighted()
  {
    final List<Pair<ClusterByKey, Integer>> keyWeights =
        KeyCollectorTestUtils.uniformRandomInverseBarbellWeightedKeys(numKeys);
    final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(
              testName,
              ClusterByStatisticsCollectorImplTest.totalWeight(
                  sortedKeyWeights,
                  new ClusterByPartition(null, null),
                  false
              ),
              collector.estimatedTotalWeight()
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  private static void verifyCollector(
      final QuantilesSketchKeyCollector collector,
      final ClusterBy clusterBy,
      final Comparator<ClusterByKey> comparator,
      final NavigableMap<ClusterByKey, List<Integer>> sortedKeyWeights
  )
  {
    KeyCollectorTestUtils.verifyCollector(
        collector,
        clusterBy,
        comparator,
        sortedKeyWeights
    );
  }
}
