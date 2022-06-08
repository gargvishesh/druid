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
import com.google.common.annotations.VisibleForTesting;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Comparator;

public class QuantilesSketchKeyCollectorFactory
    implements KeyCollectorFactory<QuantilesSketchKeyCollector, QuantilesSketchKeyCollectorSnapshot>
{
  // smallest value with normalized rank error < 0.1%; retain up to ~86k elements
  @VisibleForTesting
  static final int SKETCH_INITIAL_K = 1 << 12;

  private final Comparator<ClusterByKey> comparator;

  private QuantilesSketchKeyCollectorFactory(final Comparator<ClusterByKey> comparator)
  {
    this.comparator = comparator;
  }

  static QuantilesSketchKeyCollectorFactory create(final ClusterBy clusterBy)
  {
    return new QuantilesSketchKeyCollectorFactory(clusterBy.keyComparator());
  }

  @Override
  public QuantilesSketchKeyCollector newKeyCollector()
  {
    return new QuantilesSketchKeyCollector(comparator, ItemsSketch.getInstance(SKETCH_INITIAL_K, comparator));
  }

  @Override
  public JsonDeserializer<QuantilesSketchKeyCollectorSnapshot> snapshotDeserializer()
  {
    return new JsonDeserializer<QuantilesSketchKeyCollectorSnapshot>()
    {
      @Override
      public QuantilesSketchKeyCollectorSnapshot deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException
      {
        return jp.readValueAs(QuantilesSketchKeyCollectorSnapshot.class);
      }
    };
  }

  @Override
  public QuantilesSketchKeyCollectorSnapshot toSnapshot(QuantilesSketchKeyCollector collector)
  {
    final String encodedSketch =
        StringUtils.encodeBase64String(collector.getSketch().toByteArray(ClusterByKeySerde.INSTANCE));
    return new QuantilesSketchKeyCollectorSnapshot(encodedSketch);
  }

  @Override
  public QuantilesSketchKeyCollector fromSnapshot(QuantilesSketchKeyCollectorSnapshot snapshot)
  {
    final String encodedSketch = snapshot.getEncodedSketch();
    final byte[] bytes = StringUtils.decodeBase64String(encodedSketch);
    final ItemsSketch<ClusterByKey> sketch =
        ItemsSketch.getInstance(Memory.wrap(bytes), comparator, ClusterByKeySerde.INSTANCE);
    return new QuantilesSketchKeyCollector(comparator, sketch);
  }

  private static class ClusterByKeySerde extends ArrayOfItemsSerDe<ClusterByKey>
  {
    private static final ClusterByKeySerde INSTANCE = new ClusterByKeySerde();

    private ClusterByKeySerde()
    {
    }

    @Override
    public byte[] serializeToByteArray(final ClusterByKey[] items)
    {
      int serializedSize = Integer.BYTES * items.length;

      for (final ClusterByKey key : items) {
        serializedSize += key.array().length;
      }

      final byte[] serializedBytes = new byte[serializedSize];
      final WritableMemory writableMemory = WritableMemory.writableWrap(serializedBytes, ByteOrder.LITTLE_ENDIAN);
      long keyWritePosition = (long) Integer.BYTES * items.length;

      for (int i = 0; i < items.length; i++) {
        final ClusterByKey key = items[i];
        final byte[] keyBytes = key.array();

        writableMemory.putInt((long) Integer.BYTES * i, keyBytes.length);
        writableMemory.putByteArray(keyWritePosition, keyBytes, 0, keyBytes.length);

        keyWritePosition += keyBytes.length;
      }

      assert keyWritePosition == serializedSize;
      return serializedBytes;
    }

    @Override
    public ClusterByKey[] deserializeFromMemory(final Memory mem, final int numItems)
    {
      final ClusterByKey[] keys = new ClusterByKey[numItems];
      long keyPosition = (long) Integer.BYTES * numItems;

      for (int i = 0; i < numItems; i++) {
        final int keyLength = mem.getInt((long) Integer.BYTES * i);
        final byte[] keyBytes = new byte[keyLength];

        mem.getByteArray(keyPosition, keyBytes, 0, keyLength);
        keys[i] = ClusterByKey.wrap(keyBytes);

        keyPosition += keyLength;
      }

      return keys;
    }
  }
}
