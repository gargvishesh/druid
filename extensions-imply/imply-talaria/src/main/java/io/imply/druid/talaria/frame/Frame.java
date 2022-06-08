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
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * A data frame.
 *
 * Frames are split into contiguous "regions". With columnar frames ({@link FrameType#COLUMNAR}) each region
 * is a column. With row-based frames ({@link FrameType#ROW_BASED}) there are always two regions: row offsets
 * and row data.
 *
 * This object is lightweight. It has constant overhead regardless of the number of rows or regions.
 *
 * Frame format:
 *
 * - 1 byte: {@link FrameType#version()}
 * - 8 bytes: size in bytes of the frame, encoded as long
 * - 4 bytes: number of rows, encoded as int
 * - 4 bytes: number of regions, encoded as int
 * - 1 byte: 0 if frame is nonpermuted, 1 if frame is permuted
 * - 4 bytes x numRows: permutation section; only present for permuted frames. Array of ints mapping logical row
 * numbers to physical row numbers.
 * - 8 bytes x numRegions: region offsets. Array of longs containing the *end* offset of a region (exclusive).
 * - NNN bytes: regions, back-to-back.
 */
public class Frame
{
  public static final long HEADER_SIZE =
      Byte.BYTES /* version */ +
      Long.BYTES /* total size */ +
      Integer.BYTES /* number of rows */ +
      Integer.BYTES /* number of columns */ +
      Byte.BYTES /* permuted flag */;

  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4SafeDecompressor LZ4_DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();

  private final Memory memory;
  private final FrameType frameType;
  private final long numBytes;
  private final int numRows;
  private final int numRegions;
  private final boolean permuted;

  private Frame(Memory memory, FrameType frameType, long numBytes, int numRows, int numRegions, boolean permuted)
  {
    this.memory = memory;
    this.frameType = frameType;
    this.numBytes = numBytes;
    this.numRows = numRows;
    this.numRegions = numRegions;
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

    if (memory.getCapacity() < HEADER_SIZE) {
      throw new IAE("Memory too short for a header");
    }

    final byte version = memory.getByte(0);
    final FrameType frameType = FrameType.forVersion(version);

    if (frameType == null) {
      throw new IAE("Unexpected byte [%s] at start of frame", version);
    }

    final long numBytes = memory.getLong(Byte.BYTES);
    final int numRows = memory.getInt(Byte.BYTES + Long.BYTES);
    final int numRegions = memory.getInt(Byte.BYTES + Long.BYTES + Integer.BYTES);
    final boolean permuted = memory.getByte(Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES) != 0;

    if (numBytes != memory.getCapacity()) {
      throw new IAE("Declared size [%,d] does not match actual size [%,d]", numBytes, memory.getCapacity());
    }

    // Size of permuted row indices.
    final long rowOrderSize = (permuted ? (long) numRows * Integer.BYTES : 0);

    // Size of region ending positions.
    final long regionEndSize = (long) numRegions * Long.BYTES;

    final long expectedSizeForPreamble = HEADER_SIZE + rowOrderSize + regionEndSize;

    if (numBytes < expectedSizeForPreamble) {
      throw new IAE("Memory too short for preamble");
    }

    // Verify each region is wholly contained within this buffer.
    long regionStart = expectedSizeForPreamble; // First region starts immediately after preamble.
    long regionEnd;

    for (int regionNumber = 0; regionNumber < numRegions; regionNumber++) {
      regionEnd = memory.getLong(HEADER_SIZE + rowOrderSize + (long) regionNumber * Long.BYTES);

      if (regionEnd < regionStart) {
        throw new ISE("Region [%d] invalid: end [%,d] before start [%,d]", regionNumber, regionEnd, regionStart);
      }

      if (regionEnd < expectedSizeForPreamble || regionEnd > numBytes) {
        throw new ISE(
            "Region [%d] invalid: end [%,d] out of range [%,d -> %,d]",
            regionNumber,
            regionEnd,
            expectedSizeForPreamble,
            numBytes
        );
      }

      if (regionNumber == 0) {
        regionStart = expectedSizeForPreamble;
      } else {
        regionStart = memory.getLong(HEADER_SIZE + rowOrderSize + (long) (regionNumber - 1) * Long.BYTES);
      }

      if (regionStart < expectedSizeForPreamble || regionStart > numBytes) {
        throw new ISE(
            "Region [%d] invalid: start [%,d] out of range [%,d -> %,d]",
            regionNumber,
            regionStart,
            expectedSizeForPreamble,
            numBytes
        );
      }
    }

    return new Frame(memory, frameType, numBytes, numRows, numRegions, permuted);
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

  public FrameType type()
  {
    return frameType;
  }

  public long numBytes()
  {
    return numBytes;
  }

  public int numRows()
  {
    return numRows;
  }

  public int numRegions()
  {
    return numRegions;
  }

  public boolean isPermuted()
  {
    return permuted;
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
      final int rowPosition = memory.getInt(HEADER_SIZE + (long) Integer.BYTES * logicalRow);

      if (rowPosition < 0 || rowPosition >= numRows) {
        throw new ISE("Invalid physical row position. Corrupt frame?", logicalRow);
      }

      return rowPosition;
    } else {
      return logicalRow;
    }
  }

  /**
   * Returns memory corresponding to a particular region of this frame.
   */
  public Memory region(final int regionNumber)
  {
    if (regionNumber < 0 || regionNumber >= numRegions) {
      throw new IAE("Region [%,d] out of bounds", regionNumber);
    }

    final long regionEndPositionSectionStart =
        HEADER_SIZE + (permuted ? (long) numRows * Integer.BYTES : 0);

    final long regionStartPosition;
    final long regionEndPosition = memory.getLong(regionEndPositionSectionStart + (long) regionNumber * Long.BYTES);

    if (regionNumber == 0) {
      regionStartPosition = regionEndPositionSectionStart + (long) numRegions * Long.BYTES;
    } else {
      regionStartPosition = memory.getLong(regionEndPositionSectionStart + (long) (regionNumber - 1) * Long.BYTES);
    }

    return memory.region(regionStartPosition, regionEndPosition - regionStartPosition);
  }

  /**
   * Direct, writable access to this frame's memory. Used by operations that modify the frame in-place, like
   * {@link io.imply.druid.talaria.frame.write.FrameSort}.
   *
   * Most callers should use {@link #region} and {@link #physicalRow}, rather than this direct-access method.
   *
   * @throws IllegalStateException if this frame wraps non-writable memory
   */
  public WritableMemory writableMemory()
  {
    if (memory instanceof WritableMemory) {
      return (WritableMemory) memory;
    } else {
      throw new ISE("Frame memory is not writable");
    }
  }

  /**
   * Writes this frame to a channel, optionally compressing it as well. Returns the number of bytes written.
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
        compressedFrame = ByteBuffer.allocate(LZ4_COMPRESSOR.maxCompressedLength(Ints.checkedCast(numBytes)));

        compressedFrameLength = LZ4_COMPRESSOR.compress(
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
        final byte[] compressedFrameBytes = LZ4_COMPRESSOR.compress(frameBytes);
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
