/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.MemoryWithRange;
import io.imply.druid.talaria.frame.write.FrameWriter;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * A columnar data frame.
 *
 * This is a lightweight object: it has constant overhead regardless of the number of rows or columns in the
 * underlying frame.
 *
 * TODO(gianm): Document format
 */
public class Frame
{
  private static final LZ4SafeDecompressor LZ4_DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();

  private final Memory memory;
  private final long numBytes;
  private final int numRows;
  private final int numColumns;
  private final boolean permuted;

  private Frame(Memory memory, long numBytes, int numRows, int numColumns, boolean permuted)
  {
    this.memory = memory;
    this.numBytes = numBytes;
    this.numRows = numRows;
    this.numColumns = numColumns;
    this.permuted = permuted;
  }

  /**
   * Returns a frame backed by the provided Memory. This operation does not do any copies or allocations.
   *
   * The Memory must be in little-endian byte order.
   *
   * Behavior is undefined if the memory is modified anytime during the lifetime of the Frame object.
   */
  public static Frame wrap(final Memory memory)
  {
    if (memory.getTypeByteOrder() != ByteOrder.LITTLE_ENDIAN) {
      throw new IAE("Memory must be little-endian");
    }

    if (memory.getCapacity() < FrameWriter.HEADER_SIZE) {
      throw new IAE("Memory too short for a header");
    }

    if (memory.getByte(0) != FrameWriter.VERSION_ONE_MAGIC) {
      throw new IAE("Unexpected byte at start of frame");
    }

    final long numBytes = memory.getLong(Byte.BYTES);
    final int numRows = memory.getInt(Byte.BYTES + Long.BYTES);
    final int numColumns = memory.getInt(Byte.BYTES + Long.BYTES + Integer.BYTES);
    final boolean permuted = memory.getByte(Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES) != 0;

    if (numBytes != memory.getCapacity()) {
      throw new IAE("Declared size [%,d] does not match actual size [%,d]", numBytes, memory.getCapacity());
    }

    // Size of permuted row indices.
    final long rowOrderSize = (permuted ? (long) numRows * Integer.BYTES : 0);

    // Size of column ending positions.
    final long columnEndSize = (long) numColumns * Long.BYTES;

    final long expectedSizeForPreamble = FrameWriter.HEADER_SIZE + rowOrderSize + columnEndSize;

    if (numBytes < expectedSizeForPreamble) {
      throw new IAE("Memory too short for preamble");
    }

    // Verify each column is wholly contained within this buffer.
    long columnStart = expectedSizeForPreamble; // First column starts immediately after preamble.
    long columnEnd;

    for (int columnNumber = 0; columnNumber < numColumns; columnNumber++) {
      columnEnd = memory.getLong(FrameWriter.HEADER_SIZE + rowOrderSize + (long) columnNumber * Long.BYTES);

      if (columnEnd < columnStart) {
        throw new ISE("Column [%d] invalid: end [%,d] before start [%,d]", columnNumber, columnEnd, columnStart);
      }

      if (columnEnd < expectedSizeForPreamble || columnEnd > numBytes) {
        throw new ISE(
            "Column [%d] invalid: end [%,d] out of range [%,d -> %,d]",
            columnNumber,
            columnEnd,
            expectedSizeForPreamble,
            numBytes
        );
      }

      if (columnNumber == 0) {
        columnStart = expectedSizeForPreamble;
      } else {
        columnStart = memory.getLong(FrameWriter.HEADER_SIZE + rowOrderSize + (long) (columnNumber - 1) * Long.BYTES);
      }

      if (columnStart < expectedSizeForPreamble || columnStart > numBytes) {
        throw new ISE(
            "Column [%d] invalid: start [%,d] out of range [%,d -> %,d]",
            columnNumber,
            columnStart,
            expectedSizeForPreamble,
            numBytes
        );
      }
    }

    return new Frame(memory, numBytes, numRows, numColumns, permuted);
  }

  /**
   * Returns a frame backed by the provided ByteBuffer. This operation does not do any copies or allocations.
   *
   * The position and limit of the buffer are ignored. If you need them to be respected, call
   * {@link ByteBuffer#slice()} first, or use {@link #wrap(Memory)} to wrap a particular region.
   */
  public static Frame wrap(final ByteBuffer buffer)
  {
    return wrap(Memory.wrap(buffer, ByteOrder.LITTLE_ENDIAN));
  }

  /**
   * Returns a frame backed by the provided byte array. This operation does not do any copies or allocations.
   *
   * The position and limit of the buffer are ignored. If you need them to be respected, call
   * {@link ByteBuffer#slice()} first, or use {@link #wrap(Memory)} to wrap a particular region.
   */
  public static Frame wrap(final byte[] bytes)
  {
    // Wrap using ByteBuffer first. Even though it's seemingly unnecessary, it's cheap and it enables zero-copy
    // optimizations like the one in StringFrameColumnReader (search for "hasByteBuffer").
    return wrap(Memory.wrap(ByteBuffer.wrap(bytes), ByteOrder.LITTLE_ENDIAN));
  }

  /**
   * Decompresses the provided memory and returns a frame backed by that decompressed memory.
   *
   * This operation allocates memory on-heap to store the decompressed frame.
   */
  public static Frame decompress(final Memory memory, final long position, final long length)
  {
    if (memory.getCapacity() < position + length) {
      throw new ISE("Provided position, length is out of bounds");
    }

    if (length < Long.BYTES * 2) {
      throw new ISE("Region too short");
    }

    final int uncompressedFrameLength = Ints.checkedCast(memory.getLong(position + Long.BYTES));
    final int compressedFrameLength = Ints.checkedCast(memory.getLong(position));
    final int compressedFrameLengthFromRegionLength = Ints.checkedCast(length - Long.BYTES * 2);
    final long frameStart = position + Long.BYTES * 2;

    // Sanity check.
    if (compressedFrameLength != compressedFrameLengthFromRegionLength) {
      throw new ISE(
          "Compressed sizes disagree: [%d] (embedded) vs [%d] (region length)",
          compressedFrameLength,
          compressedFrameLengthFromRegionLength
      );
    }

    if (memory.hasByteBuffer()) {
      // Decompress directly out of the ByteBuffer.
      final ByteBuffer srcBuffer = memory.getByteBuffer();
      final ByteBuffer dstBuffer = ByteBuffer.allocate(uncompressedFrameLength);
      final int numBytesDecompressed =
          LZ4_DECOMPRESSOR.decompress(
              srcBuffer,
              Ints.checkedCast(memory.getRegionOffset() + frameStart),
              compressedFrameLength,
              dstBuffer,
              0,
              uncompressedFrameLength
          );

      // Sanity check.
      if (numBytesDecompressed != uncompressedFrameLength) {
        throw new ISE(
            "Expected to decompress [%d] bytes but got [%d] bytes",
            uncompressedFrameLength,
            numBytesDecompressed
        );
      }

      return Frame.wrap(dstBuffer);
    } else {
      // Copy first, then decompress.
      final byte[] compressedFrame = new byte[compressedFrameLength];
      memory.getByteArray(frameStart, compressedFrame, 0, compressedFrameLength);
      return Frame.wrap(LZ4_DECOMPRESSOR.decompress(compressedFrame, uncompressedFrameLength));
    }
  }

  public int numRows()
  {
    return numRows;
  }

  public long numBytes()
  {
    return numBytes;
  }

  /**
   * Maps a logical row number to a physical row number. If the frame is non-permuted, these are the same. If the frame
   * is permuted, this uses the sorted-row mappings to remap the row number.
   *
   * @throws IllegalArgumentException if "logicalRow" is out of bounds
   */
  public int physicalRow(final int logicalRow)
  {
    if (logicalRow < 0 || logicalRow >= numRows) {
      throw new IAE("Row [%,d] out of bounds", logicalRow);
    }

    if (permuted) {
      final int rowPosition = memory.getInt(FrameWriter.HEADER_SIZE + (long) Integer.BYTES * logicalRow);

      if (rowPosition < 0 || rowPosition >= numRows) {
        throw new ISE("Invalid physical row position. Corrupt frame?", logicalRow);
      }

      return rowPosition;
    } else {
      return logicalRow;
    }
  }

  public MemoryWithRange<Memory> column(final int columnNumber)
  {
    if (columnNumber < 0 || columnNumber >= numColumns) {
      throw new IAE("Column [%,d] out of bounds", columnNumber);
    }

    final long columnEndPositionSectionStart =
        FrameWriter.HEADER_SIZE + (permuted ? (long) numRows * Integer.BYTES : 0);

    final long columnStartPosition;
    final long columnEndPosition = memory.getLong(columnEndPositionSectionStart + (long) columnNumber * Long.BYTES);

    if (columnNumber == 0) {
      columnStartPosition = columnEndPositionSectionStart + (long) numColumns * Long.BYTES;
    } else {
      columnStartPosition = memory.getLong(columnEndPositionSectionStart + (long) (columnNumber - 1) * Long.BYTES);
    }

    return new MemoryWithRange<>(memory, columnStartPosition, columnEndPosition);
  }

  /**
   * TODO(gianm): Javadocs, including the fact that it returns bytes written.
   */
  public long writeTo(final WritableByteChannel channel, final boolean compress) throws IOException
  {
    if (compress) {
      // TODO(gianm): Indicator in the frame itself whether it's compressed or not
      // TODO(gianm): Handle case where numBytes > int.maxvalue? Or make it impossible.
      // TODO(gianm): Limit allocations somehow
      final ByteBuffer compressedFrame;
      final int compressedFrameLength;

      if (memory.hasByteBuffer()) {
        // Compress directly from the byte buffer.
        compressedFrame = ByteBuffer.allocate(
            FrameWriter.LZ4_COMPRESSOR.maxCompressedLength(Ints.checkedCast(numBytes))
        );

        compressedFrameLength = FrameWriter.LZ4_COMPRESSOR.compress(
            memory.getByteBuffer(),
            Ints.checkedCast(memory.getRegionOffset()),
            Ints.checkedCast(numBytes),
            compressedFrame,
            0,
            compressedFrame.remaining()
        );

        compressedFrame.limit(compressedFrameLength);
      } else {
        // Copy to byte array first, then decompress.
        final byte[] frameBytes = new byte[Ints.checkedCast(numBytes)];
        memory.getByteArray(0, frameBytes, 0, Ints.checkedCast(numBytes));
        final byte[] compressedFrameBytes = FrameWriter.LZ4_COMPRESSOR.compress(frameBytes);
        compressedFrame = ByteBuffer.wrap(compressedFrameBytes);
        compressedFrameLength = compressedFrameBytes.length;
      }

      final ByteBuffer headerBuf = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.LITTLE_ENDIAN);
      headerBuf.putLong(compressedFrameLength).putLong(numBytes).flip();
      Channels.writeFully(channel, headerBuf);
      Channels.writeFully(channel, compressedFrame);
      return Long.BYTES * 2 + compressedFrameLength;
    } else {
      memory.writeTo(0, numBytes, channel);
      return numBytes;
    }
  }
}
