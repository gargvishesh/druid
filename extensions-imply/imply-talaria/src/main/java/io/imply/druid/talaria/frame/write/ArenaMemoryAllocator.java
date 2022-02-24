/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.MemoryAllocator;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Allocator that uses a reusable {@link WritableMemory} arena. The allocator maintains a high watermark that
 * is reset to zero when all outstanding allocations have been freed.
 */
public class ArenaMemoryAllocator implements MemoryAllocator
{
  private final WritableMemory arena;
  private long allocations = 0;
  private long position = 0;

  private ArenaMemoryAllocator(WritableMemory arena)
  {
    this.arena = Preconditions.checkNotNull(arena, "arena");
  }

  public static ArenaMemoryAllocator create(final ByteBuffer buffer)
  {
    return new ArenaMemoryAllocator(WritableMemory.writableWrap(buffer));
  }

  public static ArenaMemoryAllocator createOnHeap(final int capacity)
  {
    return create(ByteBuffer.allocate(capacity));
  }

  @Override
  public Optional<ResourceHolder<WritableMemory>> allocate(final long size)
  {
    if (position + size < arena.getCapacity()) {
      final long start = position;
      allocations++;
      position += size;

      return Optional.of(
          new ResourceHolder<WritableMemory>()
          {
            private WritableMemory memory = arena.writableRegion(start, size, ByteOrder.LITTLE_ENDIAN);

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

              if (--allocations == 0) {
                // All allocations closed; reset position to enable arena reuse.
                position = 0;
              }
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
    return arena.getCapacity() - position;
  }

  @Override
  public long capacity()
  {
    return arena.getCapacity();
  }
}
