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
import io.imply.druid.talaria.frame.cluster.ClusterByTestUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.NoSuchElementException;

public class DelegateOrMinKeyCollectorTest
{
  private final ClusterBy clusterBy = new ClusterBy(ImmutableList.of(new ClusterByColumn("x", false)), 0);
  private final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
  private final Comparator<ClusterByKey> comparator = clusterBy.keyComparator();

  @Test
  public void testEmpty()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy)
        ).newKeyCollector();

    Assert.assertTrue(collector.getDelegate().isPresent());
    Assert.assertTrue(collector.isEmpty());
    Assert.assertThrows(NoSuchElementException.class, collector::minKey);
    Assert.assertEquals(0, collector.estimatedRetainedKeys());
    Assert.assertEquals(0, collector.estimatedTotalWeight());
    MatcherAssert.assertThat(collector.getDelegate().get(), CoreMatchers.instanceOf(QuantilesSketchKeyCollector.class));
  }

  @Test(expected = ISE.class)
  public void testDelegateAndMinKeyNotNullThrowsException()
  {
    ClusterBy clusterBy = ClusterBy.none();
    new DelegateOrMinKeyCollector<>(clusterBy.keyComparator(),
                                    QuantilesSketchKeyCollectorFactory.create(clusterBy).newKeyCollector(),
                                    ClusterByKey.empty());
  }

  @Test
  public void testAdd()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy)
        ).newKeyCollector();

    collector.add(createKey(1L), 1);

    Assert.assertTrue(collector.getDelegate().isPresent());
    Assert.assertFalse(collector.isEmpty());
    Assert.assertEquals(createKey(1L), collector.minKey());
    Assert.assertEquals(1, collector.estimatedRetainedKeys());
    Assert.assertEquals(1, collector.estimatedTotalWeight());
  }

  @Test
  public void testDownSampleSingleKey()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy)
        ).newKeyCollector();

    collector.add(createKey(1L), 1);
    Assert.assertTrue(collector.downSample());

    Assert.assertTrue(collector.getDelegate().isPresent());
    Assert.assertFalse(collector.isEmpty());
    Assert.assertEquals(createKey(1L), collector.minKey());
    Assert.assertEquals(1, collector.estimatedRetainedKeys());
    Assert.assertEquals(1, collector.estimatedTotalWeight());

    // Should not have actually downsampled, because the quantiles-based collector does nothing when
    // downsampling on a single key.
    Assert.assertEquals(
        QuantilesSketchKeyCollectorFactory.SKETCH_INITIAL_K,
        collector.getDelegate().get().getSketch().getK()
    );
  }

  @Test
  public void testDownSampleTwoKeys()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy)
        ).newKeyCollector();

    collector.add(createKey(1L), 1);
    collector.add(createKey(1L), 1);

    Assert.assertTrue(collector.getDelegate().isPresent());
    Assert.assertFalse(collector.isEmpty());
    Assert.assertEquals(createKey(1L), collector.minKey());
    Assert.assertEquals(2, collector.estimatedRetainedKeys());
    Assert.assertEquals(2, collector.estimatedTotalWeight());

    while (collector.getDelegate().isPresent()) {
      Assert.assertTrue(collector.downSample());
    }

    Assert.assertFalse(collector.getDelegate().isPresent());
    Assert.assertFalse(collector.isEmpty());
    Assert.assertEquals(createKey(1L), collector.minKey());
    Assert.assertEquals(1, collector.estimatedRetainedKeys());
    Assert.assertEquals(1, collector.estimatedTotalWeight());
  }

  private ClusterByKey createKey(final Object... objects)
  {
    return ClusterByTestUtils.createKey(
        ClusterByTestUtils.createKeySignature(clusterBy.getColumns(), signature),
        objects
    );
  }
}
