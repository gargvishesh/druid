/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.MemoryAllocator;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

public class HeapMemoryAllocator implements MemoryAllocator
{
  private final long capacity;

  private long bytesAllocated = 0;

  private HeapMemoryAllocator(final long capacity)
  {
    this.capacity = capacity;
  }

  public static HeapMemoryAllocator unlimited()
  {
    return new HeapMemoryAllocator(Long.MAX_VALUE);
  }

  public static HeapMemoryAllocator createWithCapacity(final long capacity)
  {
    if (capacity <= 0) {
      throw new ISE("Capacity must be greater than zero");
    }

    return new HeapMemoryAllocator(capacity);
  }

  @Override
  public Optional<ResourceHolder<WritableMemory>> allocate(final long size)
  {
    if (bytesAllocated < capacity - size) {
      bytesAllocated += size;

      return Optional.of(
          new ResourceHolder<WritableMemory>()
          {
            private WritableMemory memory =
                WritableMemory.writableWrap(ByteBuffer.allocate(Ints.checkedCast(size)).order(ByteOrder.LITTLE_ENDIAN));

            @Override
            public WritableMemory get()
            {
              if (memory == null) {
                throw new ISE("Already closed");
              }

              return memory;
            }

            @Override
            public void close()
            {
              memory = null;
              bytesAllocated -= size;
            }
          }
      );
    } else {
      return Optional.empty();
    }
  }

  @Override
  public long available()
  {
    return capacity;
  }
}
