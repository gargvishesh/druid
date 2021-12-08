/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryWithRange;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.write.HeapMemoryAllocator;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * TODO(gianm): Javadoc, including format details
 * TODO(gianm): Consistent naming for "footer" (we call it the last 2 ints; ReadableByteChunksFrameChannel calls it everything after the frames)
 * TODO(gianm): Compression...?
 */
public class FrameFileWriter implements Closeable
{
  public static final byte[] MAGIC = {(byte) 0xff, 0x01};
  public static final byte MARKER_FRAME = (byte) 0x01;
  public static final byte MARKER_NO_MORE_FRAMES = (byte) 0x02;
  public static final int FOOTER_LENGTH = Integer.BYTES * 2;

  private final WritableByteChannel channel;
  private final AppendableMemory tableOfContents;
  private final AppendableMemory partitions;
  private long bytesWritten = 0;
  private int numFrames = 0;
  private boolean usePartitions = true;
  private boolean closed = false;

  private FrameFileWriter(
      final WritableByteChannel channel,
      final AppendableMemory tableOfContents,
      final AppendableMemory partitions
  )
  {
    this.channel = channel;
    this.tableOfContents = tableOfContents;
    this.partitions = partitions;
  }

  public static FrameFileWriter open(final WritableByteChannel channel)
  {
    // TODO(gianm): Limit this somehow... probably by a max number of frames per frame file.
    final HeapMemoryAllocator allocator = HeapMemoryAllocator.unlimited();
    return new FrameFileWriter(
        channel,
        AppendableMemory.create(allocator),
        AppendableMemory.create(allocator)
    );
  }

  public void writeFrame(final Frame frame, final int partition) throws IOException
  {
    if (numFrames == Integer.MAX_VALUE) {
      throw new ISE("Too many frames");
    }

    if (partition < 0 && numFrames == 0) {
      usePartitions = false;
    }

    if (partition >= 0 != usePartitions) {
      throw new ISE("Cannot mix partitioned and non-partitioned data");
    }

    if (!tableOfContents.reserve(Long.BYTES)) {
      // Not likely to happen due to allocator limit of Long.MAX_VALUE.
      throw new ISE("Too many frames");
    }

    writeMagicIfNeeded();

    Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{MARKER_FRAME}));
    bytesWritten++;
    bytesWritten += frame.writeTo(channel, true);

    // Write *end* of frame to tableOfContents.
    final MemoryWithRange<WritableMemory> tocCursor = tableOfContents.cursor();
    tocCursor.memory().putLong(tocCursor.start(), bytesWritten);
    tableOfContents.advanceCursor(Long.BYTES);

    if (usePartitions) {
      // Write new partition if needed.
      int highestPartitionWritten = Ints.checkedCast(partitions.size() / Integer.BYTES) - 1;

      if (partition < highestPartitionWritten) {
        // Sanity check. Partition number cannot go backwards.
        throw new ISE("Partition [%,d] < highest partition [%,d]", partition, highestPartitionWritten);
      }

      while (partition > highestPartitionWritten) {
        if (!partitions.reserve(Integer.BYTES)) {
          // Not likely to happen due to allocator limit of Long.MAX_VALUE. But, if this happens, the file is corrupt.
          // Throw an error so the caller knows it is bad.
          throw new ISE("Too many partitions");
        }

        final MemoryWithRange<WritableMemory> partitionCursor = partitions.cursor();
        highestPartitionWritten++;
        partitionCursor.memory().putInt(partitionCursor.start(), numFrames);
        partitions.advanceCursor(Integer.BYTES);
      }
    }

    numFrames++;
  }

  /**
   * Stops writing this file and closes early. Callers will be able to detect that the file is truncated.
   *
   * After calling this method, {@link #close()} will do nothing. It is OK to either call it or not call it.
   */
  public void abort() throws IOException
  {
    if (!closed) {
      partitions.close();
      tableOfContents.close();
      channel.close();
      closed = true;
    }
  }

  @Override
  public void close() throws IOException
  {
    if (closed) {
      // Already closed things in abort().
      return;
    }

    // TODO(gianm): Checksum?
    writeMagicIfNeeded();

    if (!tableOfContents.reserve(FOOTER_LENGTH)) {
      throw new ISE("Can't finish table of contents");
    }
    final MemoryWithRange<WritableMemory> tocCursor = tableOfContents.cursor();
    tocCursor.memory().putInt(tocCursor.start(), numFrames);
    tocCursor.memory().putInt(tocCursor.start() + Integer.BYTES, Ints.checkedCast(partitions.size() / Integer.BYTES));
    tableOfContents.advanceCursor(FOOTER_LENGTH);
    channel.write(ByteBuffer.wrap(new byte[]{MARKER_NO_MORE_FRAMES}));
    partitions.writeTo(channel);
    partitions.close();
    tableOfContents.writeTo(channel);
    tableOfContents.close();
    channel.close();
    closed = true;
  }

  private void writeMagicIfNeeded() throws IOException
  {
    if (numFrames == 0) {
      Channels.writeFully(channel, ByteBuffer.wrap(MAGIC));
      bytesWritten += MAGIC.length;
    }
  }
}
