/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import it.unimi.dsi.fastutil.objects.Object2LongRBTreeMap;
import org.apache.druid.collections.SerializablePair;

import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Collectors;

public class DistinctKeyCollectorFactory implements KeyCollectorFactory<DistinctKeyCollector, DistinctKeySnapshot>
{
  private final Comparator<ClusterByKey> comparator;

  private DistinctKeyCollectorFactory(Comparator<ClusterByKey> comparator)
  {
    this.comparator = comparator;
  }

  static DistinctKeyCollectorFactory create(final ClusterBy clusterBy)
  {
    return new DistinctKeyCollectorFactory(clusterBy.keyComparator());
  }

  @Override
  public DistinctKeyCollector newKeyCollector()
  {
    return new DistinctKeyCollector(comparator);
  }

  @Override
  public JsonDeserializer<DistinctKeySnapshot> snapshotDeserializer()
  {
    return new JsonDeserializer<DistinctKeySnapshot>()
    {
      @Override
      public DistinctKeySnapshot deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
      {
        return jp.readValueAs(DistinctKeySnapshot.class);
      }
    };
  }

  @Override
  public DistinctKeySnapshot toSnapshot(final DistinctKeyCollector collector)
  {
    return new DistinctKeySnapshot(
        collector.getRetainedKeys()
                 .entrySet()
                 .stream()
                 .map(entry -> new SerializablePair<>(entry.getKey(), entry.getValue()))
                 .collect(Collectors.toList()),
        collector.getSpaceReductionFactor()
    );
  }

  @Override
  public DistinctKeyCollector fromSnapshot(final DistinctKeySnapshot snapshot)
  {
    final Object2LongRBTreeMap<ClusterByKey> retainedKeys = new Object2LongRBTreeMap<>(comparator);
    retainedKeys.putAll(snapshot.getKeysAsMap());
    return new DistinctKeyCollector(comparator, retainedKeys, snapshot.getSpaceReductionFactor());
  }
}
