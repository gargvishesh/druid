/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;

public class BufferToWritableMemoryCache
{
  private ByteBuffer lastAccessedBuffer;
  private WritableMemory lastAccessedMem;
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();

  @SuppressWarnings("ObjectEquality")
  public WritableMemory getMemory(ByteBuffer buffer)
  {
    if (buffer == lastAccessedBuffer) {
      return lastAccessedMem;
    }
    lastAccessedMem = memCache.computeIfAbsent(buffer, buf -> WritableMemory.writableWrap(buf, ByteOrder.nativeOrder()));
    lastAccessedBuffer = buffer;
    return lastAccessedMem;
  }
}
