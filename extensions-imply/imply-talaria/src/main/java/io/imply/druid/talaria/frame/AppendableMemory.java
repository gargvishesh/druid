/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A class that allows writing to a series of Memory blocks as if they are one big coherent chunk of memory. Memory
 * is allocated along the way using a {@link MemoryAllocator}. Useful for situations where you don't know ahead of time
 * exactly how much memory you'll need.
 */
public class AppendableMemory implements Closeable
{
  private static final int NO_BLOCK = -1;

  // Reasonable initial allocation size. Multiple of 4, 5, 8, and 9; meaning int, int + byte, long, and long + byte can
  // all be packed into blocks (see blocksPackedAndInitialSize).
  private static final int DEFAULT_INITIAL_ALLOCATION_SIZE = 360;

  // Largest allocation that we will do, unless "reserve" is called with a number bigger than this.
  private static final int SOFT_MAXIMUM_ALLOCATION_SIZE = DEFAULT_INITIAL_ALLOCATION_SIZE * 4096;

  private final MemoryAllocator allocator;
  private int nextAllocationSize;

  // One holder for every Memory we've allocated.
  private final List<ResourceHolder<WritableMemory>> blockHolders = new ArrayList<>();

  // The amount of space that has been used from each Memory block. Same length as "memoryHolders".
  private final IntList limits = new IntArrayList();

  // The global starting position for each Memory block (blockNumber -> position). Same length as "memoryHolders".
  private final LongArrayList globalStartPositions = new LongArrayList();

  // Whether the blocks we've allocated are "packed"; meaning all non-final block limits equal the allocationSize.
  private boolean blocksPackedAndInitialSize = true;

  // Return value of cursor().
  private final MemoryRange<WritableMemory> cursor;

  private AppendableMemory(final MemoryAllocator allocator, final int initialAllocationSize)
  {
    this.allocator = allocator;
    this.nextAllocationSize = initialAllocationSize;
    this.cursor = new MemoryRange<>(null, 0, 0);
  }

  /**
   * Creates an appendable memory instance with a default initial allocation size. This default size can accept
   * is a multiple of 4, 5, 8, and 9, meaning that {@link #reserve} can accept allocations of that size and
   * remain fully-packed.
   */
  public static AppendableMemory create(final MemoryAllocator allocator)
  {
    return new AppendableMemory(allocator, DEFAULT_INITIAL_ALLOCATION_SIZE);
  }

  /**
   * Creates an appendable memory instance using a particular initial allocation size.
   */
  public static AppendableMemory create(final MemoryAllocator allocator, final int initialAllocationSize)
  {
    return new AppendableMemory(allocator, initialAllocationSize);
  }

  /**
   * Return a pointer to the current cursor location, which is where the next elements should be written. The returned
   * object is updated on calls to {@link #advanceCursor} and {@link #rewindCursor}.
   *
   * The start of the returned range is the cursor location; the end is the end of the current Memory block.
   *
   * The returned Memory object is in little-endian order.
   */
  public MemoryRange<WritableMemory> cursor()
  {
    return cursor;
  }

  /**
   * Ensure that at least "bytes" amount of space is available after the cursor. Allocates a new block if needed.
   * Note: the amount of bytes is guaranteed to be in a *single* block.
   *
   * Does not move the cursor forward.
   *
   * @return true if reservation was successful, false otherwise.
   */
  public boolean reserve(final int bytes)
  {
    if (bytes < 0) {
      throw new IAE("Cannot reserve negative bytes");
    }

    if (bytes == 0) {
      return true;
    }

    if (bytes > allocator.available()) {
      return false;
    }

    final int idx = blockHolders.size() - 1;

    if (idx < 0 || bytes + limits.getInt(idx) > blockHolders.get(idx).get().getCapacity()) {
      // Allocation needed.
      // Math.max(allocationSize, bytes) in case "bytes" is greater than SOFT_MAXIMUM_ALLOCATION_SIZE.
      final Optional<ResourceHolder<WritableMemory>> newMemory =
          allocator.allocate(Math.max(nextAllocationSize, bytes));

      if (!newMemory.isPresent()) {
        return false;
      } else if (newMemory.get().get().getCapacity() < bytes) {
        // Not enough space in the allocation.
        newMemory.get().close();
        return false;
      } else {
        addBlock(newMemory.get());

        if (!blocksPackedAndInitialSize && nextAllocationSize < SOFT_MAXIMUM_ALLOCATION_SIZE) {
          // Increase size each time we add an allocation, to minimize the number of overall allocations.
          nextAllocationSize *= 2;
        }
      }
    }

    return true;
  }

  /**
   * Advances the cursor a certain number of bytes. This number of bytes must not exceed the space available in the
   * current block. Typically, it is used to commit the memory most recently reserved by {@link #reserve}.
   */
  public void advanceCursor(final int bytes)
  {
    final int blockNumber = currentBlockNumber();

    if (blockNumber < 0) {
      throw new ISE("No memory; must call 'reserve' first");
    }

    final int currentLimit = limits.getInt(blockNumber);
    final WritableMemory currentBlockMemory = blockHolders.get(blockNumber).get();
    final long available = currentBlockMemory.getCapacity() - currentLimit;

    if (bytes > available) {
      throw new IAE(
          "Cannot advance [%d] bytes; current block only has [%d] additional bytes",
          bytes,
          available
      );
    }

    final int newLimit = currentLimit + bytes;
    limits.set(blockNumber, newLimit);
    cursor.set(currentBlockMemory, newLimit, currentBlockMemory.getCapacity() - newLimit);
  }

  /**
   * Rewinds the cursor a certain number of bytes, effectively erasing them. This number of bytes must not exceed
   * the current block. Typically, it is used to erase the memory most recently advanced by {@link #advanceCursor}.
   */
  public void rewindCursor(final int bytes)
  {
    if (bytes < 0) {
      throw new IAE("bytes < 0");
    } else if (bytes == 0) {
      return;
    }

    final int blockNumber = currentBlockNumber();

    if (blockNumber < 0) {
      throw new ISE("No memory; must call 'reserve' first");
    }

    final int currentLimit = limits.getInt(blockNumber);

    if (bytes > currentLimit) {
      throw new IAE("Cannot rewind [%d] bytes; current block is only [%d] bytes long", bytes, currentLimit);
    }

    final int newLimit = currentLimit - bytes;
    limits.set(blockNumber, newLimit);

    final WritableMemory currentBlockMemory = blockHolders.get(blockNumber).get();
    cursor.set(currentBlockMemory, newLimit, currentBlockMemory.getCapacity() - newLimit);
  }

  public long size()
  {
    long sz = 0;

    for (int i = 0; i < limits.size(); i++) {
      sz += limits.getInt(i);
    }

    return sz;
  }

  /**
   * Write current memory to a channel.
   */
  public void writeTo(final WritableByteChannel channel) throws IOException
  {
    for (int i = 0; i < blockHolders.size(); i++) {
      final ResourceHolder<WritableMemory> memoryHolder = blockHolders.get(i);
      final WritableMemory memory = memoryHolder.get();
      final int limit = limits.getInt(i);

      if (memory.hasByteBuffer()) {
        final ByteBuffer byteBuffer = memory.getByteBuffer().duplicate();
        byteBuffer.limit(Ints.checkedCast(memory.getRegionOffset(limit)));
        byteBuffer.position(Ints.checkedCast(memory.getRegionOffset(0)));
        Channels.writeFully(channel, byteBuffer);
      } else {
        // No implementation currently for Memory without backing ByteBuffer. (It's never needed.)
        throw new UnsupportedOperationException("Cannot write Memory without backing ByteBuffer");
      }
    }
  }

  /**
   * Write current memory to a {@link WritableMemory} buffer.
   */
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    long currentPosition = startPosition;

    for (int i = 0; i < blockHolders.size(); i++) {
      final ResourceHolder<WritableMemory> memoryHolder = blockHolders.get(i);
      final WritableMemory srcMemory = memoryHolder.get();
      final int limit = limits.getInt(i);
      srcMemory.copyTo(0, memory, currentPosition, limit);
      currentPosition += limit;
    }

    return currentPosition - startPosition;
  }

  public void clear()
  {
    blockHolders.forEach(ResourceHolder::close);
    blockHolders.clear();
    limits.clear();
    globalStartPositions.clear();
    cursor.set(null, 0, 0);
  }

  @Override
  public void close()
  {
    clear();
  }

  private void addBlock(final ResourceHolder<WritableMemory> block)
  {
    final int lastBlockNumber = currentBlockNumber();
    final WritableMemory blockMemory = block.get();

    if (lastBlockNumber == NO_BLOCK) {
      globalStartPositions.add(0);
    } else {
      final int lastBlockLimit = limits.getInt(lastBlockNumber);
      final long newBlockGlobalStartPosition = globalStartPositions.getLong(lastBlockNumber) + lastBlockLimit;
      globalStartPositions.add(newBlockGlobalStartPosition);

      if (blockMemory.getCapacity() != DEFAULT_INITIAL_ALLOCATION_SIZE
          || lastBlockLimit != DEFAULT_INITIAL_ALLOCATION_SIZE) {
        blocksPackedAndInitialSize = false;
      }
    }

    blockHolders.add(block);
    limits.add(0);
    cursor.set(blockMemory, 0, blockMemory.getCapacity());
  }

  private int currentBlockNumber()
  {
    if (blockHolders.isEmpty()) {
      return NO_BLOCK;
    } else {
      return blockHolders.size() - 1;
    }
  }
}
