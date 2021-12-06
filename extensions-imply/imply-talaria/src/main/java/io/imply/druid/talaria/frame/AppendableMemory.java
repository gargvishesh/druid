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
  // all be packed into blocks without wasted space.
  private static final int INITIAL_ALLOCATION_SIZE = 360;

  // Largest allocation that we will do, unless "reserve" is called with a number bigger than this.
  private static final int SOFT_MAXIMUM_ALLOCATION_SIZE = INITIAL_ALLOCATION_SIZE * 4096;

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

  private AppendableMemory(final MemoryAllocator allocator)
  {
    this.allocator = allocator;
    this.nextAllocationSize = INITIAL_ALLOCATION_SIZE;
  }

  public static AppendableMemory create(final MemoryAllocator allocator)
  {
    return new AppendableMemory(allocator);
  }

  /**
   * Return a pointer to the current cursor location, which is where the next elements should be written.
   *
   * The start of the returned range is the cursor location; the end is the end of the current Memory block.
   *
   * This range is a snapshot. It does not advance or rewind when the cursor advances or rewinds.
   */
  public MemoryWithRange<WritableMemory> cursor()
  {
    final int blockNumber = currentBlockNumber();

    if (blockNumber < 0) {
      throw new ISE("No memory; must call 'reserve' first");
    }

    final WritableMemory memory = blockHolders.get(blockNumber).get();
    return new MemoryWithRange<>(memory, limits.getInt(blockNumber), memory.getCapacity());
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
    final long available = blockHolders.get(blockNumber).get().getCapacity() - currentLimit;

    if (bytes > available) {
      throw new IAE(
          "Cannot advance [%d] bytes; current block only has [%d] additional bytes",
          bytes,
          available
      );
    }

    limits.set(blockNumber, currentLimit + bytes);
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

    limits.set(blockNumber, currentLimit - bytes);
  }

  /**
   * Return a pointer to the given position. The position is interpreted as a global position across all blocks, not a
   * position within a specific block.
   *
   * The start of the returned range is the requested location; the end is the limit of the Memory block that contains
   * the requested position.
   *
   * @throws IAE if nothing exists at this position
   */
  public MemoryWithRange<WritableMemory> read(final long position)
  {
    final int blockNumber = findBlock(position);
    if (blockNumber == NO_BLOCK) {
      throw new IAE("No such position");
    }

    final int memoryStartPosition = Ints.checkedCast(position - globalStartPositions.getLong(blockNumber));
    final int memoryEndPosition = limits.getInt(blockNumber);
    if (memoryStartPosition >= memoryEndPosition) {
      throw new IAE("No such position");
    }

    final WritableMemory memory = blockHolders.get(blockNumber).get();
    return new MemoryWithRange<>(memory, memoryStartPosition, memoryEndPosition);
  }

  public int getInt(final long position)
  {
    final int blockNumber = findBlock(position);
    final long blockPosition = position - globalStartPositions.getLong(blockNumber);
    assertBlockBounds(blockNumber, blockPosition, Integer.BYTES);
    return blockHolders.get(blockNumber).get().getInt(blockPosition);
  }

  public long getLong(final long position)
  {
    final int blockNumber = findBlock(position);
    final long blockPosition = position - globalStartPositions.getLong(blockNumber);
    assertBlockBounds(blockNumber, blockPosition, Long.BYTES);
    return blockHolders.get(blockNumber).get().getLong(blockPosition);
  }

  public float getFloat(final long position)
  {
    final int blockNumber = findBlock(position);
    final long blockPosition = position - globalStartPositions.getLong(blockNumber);
    assertBlockBounds(blockNumber, blockPosition, Float.BYTES);
    return blockHolders.get(blockNumber).get().getFloat(blockPosition);
  }

  public double getDouble(final long position)
  {
    final int blockNumber = findBlock(position);
    final long blockPosition = position - globalStartPositions.getLong(blockNumber);
    assertBlockBounds(blockNumber, blockPosition, Double.BYTES);
    return blockHolders.get(blockNumber).get().getDouble(blockPosition);
  }

  public long size()
  {
    long sz = 0;

    for (int i = 0; i < limits.size(); i++) {
      sz += limits.getInt(i);
    }

    return sz;
  }

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

  public void clear()
  {
    blockHolders.forEach(ResourceHolder::close);
    blockHolders.clear();
    limits.clear();
    globalStartPositions.clear();
  }

  @Override
  public void close()
  {
    clear();
  }

  private void addBlock(final ResourceHolder<WritableMemory> block)
  {
    final int lastBlockNumber = currentBlockNumber();

    if (lastBlockNumber == NO_BLOCK) {
      globalStartPositions.add(0);
    } else {
      final int lastBlockLimit = limits.getInt(lastBlockNumber);
      final long newBlockGlobalStartPosition = globalStartPositions.getLong(lastBlockNumber) + lastBlockLimit;
      globalStartPositions.add(newBlockGlobalStartPosition);

      if (block.get().getCapacity() != INITIAL_ALLOCATION_SIZE || lastBlockLimit != INITIAL_ALLOCATION_SIZE) {
        blocksPackedAndInitialSize = false;
      }
    }

    blockHolders.add(block);
    limits.add(0);
  }

  private int currentBlockNumber()
  {
    if (blockHolders.isEmpty()) {
      return NO_BLOCK;
    } else {
      return blockHolders.size() - 1;
    }
  }

  private int findBlock(final long position)
  {
    if (blocksPackedAndInitialSize) {
      long blockNumber = position / INITIAL_ALLOCATION_SIZE;
      if (blockNumber < 0 || blockNumber >= blockHolders.size()) {
        throw new IAE("No such position");
      }

      return (int) blockNumber;
    } else {
      // Variable allocations: each block is twice as large as the last, so we can search half the remaining space
      // with each iteration by looking at each block in reverse.
      for (int blockNumber = globalStartPositions.size() - 1; blockNumber >= 0; blockNumber--) {
        if (globalStartPositions.getLong(blockNumber) <= position) {
          return blockNumber;
        }
      }

      throw new IAE("No such position");
    }
  }

  /**
   * Checks that the range from the provided position (relative to block start), for the provided length, falls with
   * the limit for {@code blockNumber}.
   */
  private void assertBlockBounds(final int blockNumber, final long blockPosition, final long length)
  {
    assert blockPosition >= 0
           && length >= 0
           && blockPosition + length <= limits.getInt(blockNumber);
  }
}
