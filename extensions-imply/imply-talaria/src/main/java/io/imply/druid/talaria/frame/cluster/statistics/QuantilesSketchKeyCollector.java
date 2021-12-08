/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class QuantilesSketchKeyCollector implements KeyCollector<QuantilesSketchKeyCollector>
{
  private final Comparator<ClusterByKey> comparator;
  private ItemsSketch<ClusterByKey> sketch;

  QuantilesSketchKeyCollector(final Comparator<ClusterByKey> comparator, final ItemsSketch<ClusterByKey> sketch)
  {
    this.comparator = comparator;
    this.sketch = sketch;
  }

  @Override
  public void add(ClusterByKey key)
  {
    sketch.update(key);
  }

  @Override
  public void addAll(QuantilesSketchKeyCollector other)
  {
    final ItemsUnion<ClusterByKey> union = ItemsUnion.getInstance(comparator);
    union.update(sketch);
    union.update(other.sketch);
    sketch = union.getResultAndReset();
  }

  @Override
  public long estimatedCount()
  {
    return sketch.getN();
  }

  @Override
  public int estimatedRetainedKeys()
  {
    // Rough estimation of retained keys for a given K for ~billions of total items, based on the table from
    // https://datasketches.apache.org/docs/Quantiles/OrigQuantilesSketch.html.
    final int estimatedMaxRetainedKeys = 11 * sketch.getK();

    // Cast to int is safe because estimatedMaxKeys is always within int range.
    return (int) Math.min(sketch.getN(), estimatedMaxRetainedKeys);
  }

  @Override
  public void downSample()
  {
    if (sketch.getK() == 1) {
      throw new ISE("Too many keys (estimated = %,d; retained = %,d)", estimatedCount(), sketch.getRetainedItems());
    }

    sketch = sketch.downSample(sketch.getK() / 2);
  }

  @Override
  public ClusterByKey minKey()
  {
    final ClusterByKey minValue = sketch.getMinValue();
    if (minValue == null) {
      throw new NoSuchElementException();
    } else {
      return minValue;
    }
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetSize(long targetPartitionSize)
  {
    if (targetPartitionSize <= 0) {
      throw new IAE("targetPartitionSize must be positive, but was [%d]", targetPartitionSize);
    }

    final int numPartitions =
        Ints.checkedCast(LongMath.divide(sketch.getN(), targetPartitionSize, RoundingMode.CEILING));

    // numPartitions + 1, because the final quantile is the max, and we won't build a partition based on that.
    final ClusterByKey[] quantiles = sketch.getQuantiles(numPartitions + 1);
    final List<ClusterByPartition> partitions = new ArrayList<>();

    for (int i = 0; i < numPartitions; i++) {
      final boolean isFinalPartition = i == numPartitions - 1;

      if (isFinalPartition) {
        partitions.add(new ClusterByPartition(quantiles[i], null));
      } else {
        final ClusterByPartition partition = new ClusterByPartition(quantiles[i], quantiles[i + 1]);
        final int cmp = comparator.compare(partition.getStart(), partition.getEnd());
        if (cmp < 0) {
          // Skip partitions where start == end.
          // I don't think start can be greater than end, but if that happens, skip them too!
          partitions.add(partition);
        }
      }
    }

    return new ClusterByPartitions(partitions);
  }

  /**
   * Retrieves the backing sketch. Exists for usage by {@link QuantilesSketchKeyCollectorFactory}.
   */
  ItemsSketch<ClusterByKey> getSketch()
  {
    return sketch;
  }
}
