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
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import org.apache.druid.segment.column.RowSignature;

import java.util.Comparator;
import java.util.TreeSet;

public class DistinctKeyCollectorFactory implements KeyCollectorFactory<DistinctKeyCollector>
{
  private final Comparator<ClusterByKey> comparator;

  private DistinctKeyCollectorFactory(Comparator<ClusterByKey> comparator)
  {
    this.comparator = comparator;
  }

  static DistinctKeyCollectorFactory create(
      final ClusterBy clusterBy,
      final RowSignature signature
  )
  {
    return new DistinctKeyCollectorFactory(clusterBy.keyComparator(signature));
  }

  @Override
  public DistinctKeyCollector newKeyCollector()
  {
    return new DistinctKeyCollector(comparator);
  }

  @Override
  public Class<? extends KeyCollectorSnapshot> snapshotClass()
  {
    return DistinctKeySnapshot.class;
  }

  @Override
  public KeyCollectorSnapshot toSnapshot(final DistinctKeyCollector collector)
  {
    return new DistinctKeySnapshot(
        ImmutableList.copyOf(collector.getRetainedKeys()),
        collector.getSpaceReductionFactor()
    );
  }

  @Override
  public DistinctKeyCollector fromSnapshot(final KeyCollectorSnapshot snapshotObject)
  {
    final DistinctKeySnapshot snapshot = (DistinctKeySnapshot) snapshotObject;
    final TreeSet<ClusterByKey> retainedKeys = new TreeSet<>(comparator);
    retainedKeys.addAll(snapshot.getKeys());
    return new DistinctKeyCollector(comparator, retainedKeys, snapshot.getSpaceReductionFactor());
  }
}
