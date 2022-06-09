/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.ColumnCapacityExceededException;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedBlockSerializer implements Serializer
{
  private static final MetaSerdeHelper<CompressedBlockSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((CompressedBlockSerializer x) -> (byte) 0x01)
      .writeByte(x -> x.compression.getId())
      .writeInt(x -> CompressedPools.BUFFER_SIZE)
      .writeInt(x -> x.numBlocks);

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final CompressionStrategy compression;
  private final CompressionStrategy.Compressor compressor;

  private final ByteBuffer offsetValueConverter = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());

  private ByteBuffer uncompressedDataBuffer;
  private ByteBuffer compressedDataBuffer;
  private int numBlocks;
  private int currentOffset;

  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;

  public CompressedBlockSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,

      CompressionStrategy compression,
      int blockSize
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.compression = compression;
    this.compressor = compression.getCompressor();
    this.uncompressedDataBuffer = compressor.allocateInBuffer(blockSize, segmentWriteOutMedium.getCloser())
                                            .order(ByteOrder.nativeOrder());
    this.compressedDataBuffer = compressor.allocateOutBuffer(blockSize, segmentWriteOutMedium.getCloser())
                                          .order(ByteOrder.nativeOrder());
  }

  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  public void addValue(byte[] bytes) throws IOException
  {
    if (uncompressedDataBuffer == null) {
      throw new IllegalStateException("written out already");
    }
    flushIfNeeded();

    if (bytes.length <= uncompressedDataBuffer.remaining()) {
      uncompressedDataBuffer.put(bytes);
    } else {
      int written = 0;
      // write until we have had our fill, flushing buffers as needed
      while (written < bytes.length) {
        int next = Math.min(uncompressedDataBuffer.remaining(), bytes.length - written);
        uncompressedDataBuffer.put(bytes, written, next);
        written += next;
        flushIfNeeded();
      }
    }
  }

  public void addValue(ByteBuffer bytes) throws IOException
  {
    if (uncompressedDataBuffer == null) {
      throw new IllegalStateException("written out already");
    }
    flushIfNeeded();
    int size = bytes.remaining();
    if (size <= uncompressedDataBuffer.remaining()) {
      uncompressedDataBuffer.put(bytes);
    } else {
      ByteBuffer transferBuffer = bytes.asReadOnlyBuffer().order(bytes.order());
      while (transferBuffer.hasRemaining()) {
        int writeSize = Math.min(transferBuffer.remaining(), uncompressedDataBuffer.remaining());
        transferBuffer.limit(transferBuffer.position() + writeSize);
        uncompressedDataBuffer.put(transferBuffer);
        transferBuffer.limit(bytes.limit());
        flushIfNeeded();
      }
    }
    bytes.rewind();
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeEndBuffer();
    return META_SERDE_HELPER.size(this) + headerOut.size() + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeEndBuffer();
    META_SERDE_HELPER.writeTo(channel, this);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  public void flushIfNeeded() throws IOException
  {
    if (!uncompressedDataBuffer.hasRemaining()) {
      flushBuffer();
    }
  }

  private void flushBuffer() throws IOException
  {
    uncompressedDataBuffer.rewind();
    compressedDataBuffer.clear();

    final ByteBuffer compressed = compressor.compress(uncompressedDataBuffer, compressedDataBuffer);

    currentOffset += compressed.remaining();
    offsetValueConverter.clear();
    offsetValueConverter.putInt(currentOffset);
    offsetValueConverter.flip();
    Channels.writeFully(headerOut, offsetValueConverter);
    Channels.writeFully(valuesOut, compressed);
    uncompressedDataBuffer.clear();
    numBlocks++;
    if (numBlocks < 0) {
      throw new ColumnCapacityExceededException("compressed");
    }
  }

  private void writeEndBuffer() throws IOException
  {
    if (uncompressedDataBuffer != null) {
      uncompressedDataBuffer.flip();
      flushBuffer();
      uncompressedDataBuffer = null;
    }
  }
}
