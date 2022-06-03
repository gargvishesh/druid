/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.data.CompressionStrategy;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockCompressedPayloadReader
{
  private static final CompressionStrategy.Decompressor DECOMPRESSOR = CompressionStrategy.LZ4.getDecompressor();

  private final IntIndexView blockIndexView;
  private final ByteBuffer compressedBlocksByteBuffer;
  private final ByteBuffer uncompressedByteBuffer;
  private final int blockSize;
  private final long maxValidUncompressedOffset;

  private int currentUncompressedBlockNumber = -1;

  public BlockCompressedPayloadReader(
      IntIndexView blockIndexView,
      ByteBuffer compressedBlocksByteBuffer,
      ByteBuffer uncompressedByteBuffer
  )
  {
    this.blockIndexView = blockIndexView;
    this.compressedBlocksByteBuffer = compressedBlocksByteBuffer;
    this.uncompressedByteBuffer = uncompressedByteBuffer;
    uncompressedByteBuffer.clear();
    blockSize = uncompressedByteBuffer.remaining();
    maxValidUncompressedOffset = Integer.MAX_VALUE * (long) blockSize;
  }

  public static BlockCompressedPayloadReader create(ByteBuffer originalByteBuffer, ByteBuffer uncompressedByteBuffer)
  {
    ByteBuffer masterByteBuffer = originalByteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

    int blockIndexSize = masterByteBuffer.getInt(masterByteBuffer.position());
    ByteBuffer blockIndexBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
    blockIndexBuffer.position(masterByteBuffer.position() + Integer.BYTES);
    blockIndexBuffer.limit(blockIndexBuffer.position() + blockIndexSize);

    int dataStreamSize = masterByteBuffer.getInt(blockIndexBuffer.limit());
    ByteBuffer compressedBlockStreamByteBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
    compressedBlockStreamByteBuffer.position(blockIndexBuffer.limit() + Integer.BYTES);
    compressedBlockStreamByteBuffer.limit(compressedBlockStreamByteBuffer.position() + dataStreamSize);

    return new BlockCompressedPayloadReader(
        new IntIndexView(blockIndexBuffer),
        compressedBlockStreamByteBuffer,
        uncompressedByteBuffer
    );
  }

  public ByteBuffer read(long uncompressedStart, int size)
  {
    if (size == 0) {
      return SimpleTimeSeriesView.NULL_ROW;
    }
    Preconditions.checkArgument(uncompressedStart + size < maxValidUncompressedOffset);
    int blockNumber = (int) (uncompressedStart / blockSize);
    int blockOffset = (int) (uncompressedStart % blockSize);
    ByteBuffer currentUncompressedBlock = getUncompressedBlock(blockNumber);

    currentUncompressedBlock.position(blockOffset);

    if (size <= currentUncompressedBlock.remaining()) {
      ByteBuffer resultByteBuffer = currentUncompressedBlock.asReadOnlyBuffer().order(currentUncompressedBlock.order());
      resultByteBuffer.limit(blockOffset + size);

      return resultByteBuffer;
    } else {
      byte[] payload = readMultiBlock(size, blockNumber, blockOffset);

      return ByteBuffer.wrap(payload).order(ByteOrder.nativeOrder());
    }
  }

  @Nonnull
  private byte[] readMultiBlock(int size, int blockNumber, int blockOffset)
  {
    byte[] payload = new byte[size];
    int bytesRead = 0;

    do {
      ByteBuffer currentUncompressedBlock = getUncompressedBlock(blockNumber);

      currentUncompressedBlock.position(blockOffset);

      int readSizeBytes = Math.min(size - bytesRead, currentUncompressedBlock.remaining());

      currentUncompressedBlock.get(payload, bytesRead, readSizeBytes);
      bytesRead += readSizeBytes;
      blockNumber++;
      blockOffset = 0;
    } while (bytesRead < size);

    return payload;
  }

  private ByteBuffer getUncompressedBlock(int blockNumber)
  {
    if (currentUncompressedBlockNumber != blockNumber) {
      // if blockNumber is the last valid block, there is always an extra entry which is the position of the next
      // start for an additional block, ie length of total compressed block stream
      int blockStart = blockIndexView.getStart(blockNumber);
      int nextBlockStart = blockIndexView.getStart(blockNumber + 1);
      int compressedBlockSize = nextBlockStart - blockStart;
      int blockPosition = compressedBlocksByteBuffer.position() + blockStart;
      ByteBuffer compressedBlock = compressedBlocksByteBuffer.asReadOnlyBuffer()
                                                             .order(compressedBlocksByteBuffer.order());
      compressedBlock.position(blockPosition);
      compressedBlock.limit(compressedBlock.position() + compressedBlockSize);
      uncompressedByteBuffer.clear();

      DECOMPRESSOR.decompress(compressedBlock, compressedBlockSize, uncompressedByteBuffer);
      currentUncompressedBlockNumber = blockNumber;
    }

    return uncompressedByteBuffer;
  }
}
