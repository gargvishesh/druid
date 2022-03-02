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

import javax.annotation.Nullable;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A key collector that is used when not aggregating. It uses a quantiles sketch to track keys.
 */
public class QuantilesSketchKeyCollector implements KeyCollector<QuantilesSketchKeyCollector>
{
  private final Comparator<ClusterByKey> comparator;
  private ItemsSketch<ClusterByKey> sketch;

  QuantilesSketchKeyCollector(
      final Comparator<ClusterByKey> comparator,
      @Nullable final ItemsSketch<ClusterByKey> sketch
  )
  {
    this.comparator = comparator;
    this.sketch = sketch;
  }

  @Override
  public void add(ClusterByKey key, long weight)
  {
    for (int i = 0; i < weight; i++) {
      // Add the same key multiple times to make it "heavier".
      sketch.update(key);
    }
  }

  @Override
  public void addAll(QuantilesSketchKeyCollector other)
  {
    final ItemsUnion<ClusterByKey> union = ItemsUnion.getInstance(
        Math.max(sketch.getK(), other.sketch.getK()),
        comparator
    );

    union.update(sketch);
    union.update(other.sketch);
    sketch = union.getResultAndReset();
  }

  @Override
  public boolean isEmpty()
  {
    return sketch.isEmpty();
  }

  @Override
  public long estimatedTotalWeight()
  {
    return sketch.getN();
  }

  @Override
  public int estimatedRetainedKeys()
  {
    // Rough estimation of retained keys for a given K for ~billions of total items, based on the table from
    // https://datasketches.apache.org/docs/Quantiles/OrigQuantilesSketch.html.
    final int estimatedMaxRetainedKeys = 11 * sketch.getK();

    // Cast to int is safe because estimatedMaxRetainedKeys is always within int range.
    return (int) Math.min(sketch.getN(), estimatedMaxRetainedKeys);
  }

  @Override
  public boolean downSample()
  {
    if (sketch.getN() <= 1) {
      return true;
    } else if (sketch.getK() == 2) {
      return false;
    } else {
      sketch = sketch.downSample(sketch.getK() / 2);
      return true;
    }
  }

  @Override
  public ClusterByKey minKey()
  {
    final ClusterByKey minValue = sketch.getMinValue();

    if (minValue != null) {
      return minValue;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetWeight(final long targetWeight)
  {
    if (targetWeight <= 0) {
      throw new IAE("targetPartitionWeight must be positive, but was [%d]", targetWeight);
    }

    if (sketch.getN() == 0) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final int numPartitions = Ints.checkedCast(LongMath.divide(sketch.getN(), targetWeight, RoundingMode.CEILING));

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
