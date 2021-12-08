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
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByKeyDeserializer;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.Comparator;

public class QuantilesSketchKeyCollectorFactory implements KeyCollectorFactory<QuantilesSketchKeyCollector>
{
  // smallest value with normalized rank error < 0.1%; retain up to ~86k elements
  private static final int SKETCH_INITIAL_K = 1 << 12;

  private final Comparator<ClusterByKey> comparator;
  private final ArrayOfKeysSerDe arrayOfKeysSerDe;

  private QuantilesSketchKeyCollectorFactory(
      final Comparator<ClusterByKey> comparator,
      final ArrayOfKeysSerDe arrayOfKeysSerDe
  )
  {
    this.comparator = comparator;
    this.arrayOfKeysSerDe = arrayOfKeysSerDe;
  }

  static QuantilesSketchKeyCollectorFactory create(final ClusterBy clusterBy, final RowSignature signature)
  {
    final ObjectMapper objectMapper = new SmileMapper();

    objectMapper.registerModule(
        new SimpleModule().addDeserializer(
            ClusterByKey.class,
            ClusterByKeyDeserializer.fromClusterByAndRowSignature(clusterBy, signature)
        )
    );

    return new QuantilesSketchKeyCollectorFactory(
        clusterBy.keyComparator(signature),
        new ArrayOfKeysSerDe(objectMapper)
    );
  }

  @Override
  public QuantilesSketchKeyCollector newKeyCollector()
  {
    return new QuantilesSketchKeyCollector(comparator, ItemsSketch.getInstance(SKETCH_INITIAL_K, comparator));
  }

  @Override
  public Class<? extends KeyCollectorSnapshot> snapshotClass()
  {
    return QuantilesSketchKeyCollectorSnapshot.class;
  }

  @Override
  public KeyCollectorSnapshot toSnapshot(QuantilesSketchKeyCollector collector)
  {
    final String encodedSketch = StringUtils.encodeBase64String(collector.getSketch().toByteArray(arrayOfKeysSerDe));
    return new QuantilesSketchKeyCollectorSnapshot(encodedSketch);
  }

  @Override
  public QuantilesSketchKeyCollector fromSnapshot(KeyCollectorSnapshot snapshot)
  {
    final String encodedSketch = ((QuantilesSketchKeyCollectorSnapshot) snapshot).getEncodedSketch();
    final byte[] bytes = StringUtils.decodeBase64String(encodedSketch);
    final ItemsSketch<ClusterByKey> sketch =
        ItemsSketch.getInstance(Memory.wrap(bytes), comparator, arrayOfKeysSerDe);
    return new QuantilesSketchKeyCollector(comparator, sketch);
  }

  private static class ArrayOfKeysSerDe extends ArrayOfItemsSerDe<ClusterByKey>
  {
    private final ObjectMapper objectMapper;

    private ArrayOfKeysSerDe(final ObjectMapper objectMapper)
    {
      this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serializeToByteArray(final ClusterByKey[] items)
    {
      try {
        return objectMapper.writeValueAsBytes(items);
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public ClusterByKey[] deserializeFromMemory(final Memory mem, final int numItems)
    {
      try {
        final byte[] bytes = new byte[Ints.checkedCast(mem.getCapacity())];
        mem.getByteArray(0, bytes, 0, bytes.length);
        final ClusterByKey[] keys = objectMapper.readValue(bytes, ClusterByKey[].class);
        if (keys.length != numItems) {
          throw new ISE("Key count mismatch: expected [%d], got [%d]", numItems, keys.length);
        }
        return keys;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
