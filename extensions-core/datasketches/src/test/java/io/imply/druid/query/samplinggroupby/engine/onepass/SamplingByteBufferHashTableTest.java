/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.onepass;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.theta.RawHashHeapQuickSelectSketch;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class SamplingByteBufferHashTableTest
{
  @Test
  public void testNoResize()
  {
    int insertionCount = 12;
    RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch = RawHashHeapQuickSelectSketch.create(
        16,
        Util.DEFAULT_UPDATE_SEED
    );
    SamplingByteBufferHashTable samplingByteBufferHashTable = new SamplingByteBufferHashTable(
        1.0F,
        16,
        8,
        ByteBuffer.allocate(100),
        4,
        Integer.MAX_VALUE,
        null,
        new Int2LongOpenHashMap(4),
        rawHashHeapQuickSelectSketch,
        16,
        theta -> {
          throw new RuntimeException("Resize Happened");
        }
    );
    samplingByteBufferHashTable.reset();
    for (int i = 0; i < insertionCount; i++) { // works till 12 since max buckets allowed = 100 / 8
      ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{(byte) i});
      int bucket = samplingByteBufferHashTable.findBucketWithAutoGrowth(byteBuffer, getAggregateHash(i), () -> {});
      samplingByteBufferHashTable.initializeNewBucketKey(bucket, byteBuffer, i);
      rawHashHeapQuickSelectSketch.updateHash(getRawHash(i));
    }
    Assert.assertEquals(insertionCount, samplingByteBufferHashTable.getSize());
  }

  @Test
  public void testResizeWithoutSketchFilter()
  {
    int insertionCount = 15;
    Int2LongOpenHashMap map = new Int2LongOpenHashMap(insertionCount);
    AtomicInteger resizeTriggerCount = new AtomicInteger(0);
    RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch = RawHashHeapQuickSelectSketch.create(
        16, // sketch can accommodate 16 elements so filtering will happen for 15 insertions
        Util.DEFAULT_UPDATE_SEED
    );
    SamplingByteBufferHashTable samplingByteBufferHashTable = new SamplingByteBufferHashTable(
        1.0F,
        4, // initial buckets set to 4 to trigger resize
        8,
        ByteBuffer.allocate(250), // buffer is 250 to accommodate init buckets + resize buckets
        4,
        Integer.MAX_VALUE,
        null,
        map,
        rawHashHeapQuickSelectSketch,
        16,
        theta -> resizeTriggerCount.getAndIncrement()
    );
    samplingByteBufferHashTable.reset();
    for (int i = 0; i < insertionCount; i++) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{(byte) i});
      int bucket = samplingByteBufferHashTable.findBucketWithAutoGrowth(byteBuffer, getAggregateHash(i), () -> {});
      map.put(bucket * 8, getRawHash(i));
      samplingByteBufferHashTable.initializeNewBucketKey(bucket, byteBuffer, i);
      rawHashHeapQuickSelectSketch.updateHash(getRawHash(i));
    }
    Assert.assertEquals(2, resizeTriggerCount.get());
    Assert.assertEquals(insertionCount, samplingByteBufferHashTable.getSize());
  }

  @Test
  public void testResizeWithSketchFilter()
  {
    int insertionCount = 35;
    Int2LongOpenHashMap map = new Int2LongOpenHashMap(insertionCount);
    RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch = RawHashHeapQuickSelectSketch.create(
        16,
        Util.DEFAULT_UPDATE_SEED
    );
    AtomicInteger resizeTriggerCount = new AtomicInteger(0);
    SamplingByteBufferHashTable samplingByteBufferHashTable = new SamplingByteBufferHashTable(
        1.0F,
        4, // initial buckets set to 4 to trigger resize
        8,
        ByteBuffer.allocate(1000),
        4,
        Integer.MAX_VALUE,
        null,
        map,
        rawHashHeapQuickSelectSketch,
        16,
        theta -> resizeTriggerCount.getAndIncrement()
    );
    samplingByteBufferHashTable.reset();
    for (int i = 0; i < insertionCount; i++) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{(byte) i});
      int bucket = samplingByteBufferHashTable.findBucketWithAutoGrowth(byteBuffer, getAggregateHash(i), () -> {});
      map.put(bucket * 8, getRawHash(i));
      samplingByteBufferHashTable.initializeNewBucketKey(bucket, byteBuffer, i);
      rawHashHeapQuickSelectSketch.updateHash(getRawHash(i));
    }
    Assert.assertEquals(4, resizeTriggerCount.get());
    // 19 -> 16 retained groups after the resizing from 32 + 3 new insertion from 32-35
    Assert.assertEquals(19, samplingByteBufferHashTable.getSize());
  }

  public static long getRawHash(int i)
  {
    return (MurmurHash3.hash(i, Util.DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  public static int getAggregateHash(int i)
  {
    return ((int) getRawHash(i)) >>> 1;
  }
}
