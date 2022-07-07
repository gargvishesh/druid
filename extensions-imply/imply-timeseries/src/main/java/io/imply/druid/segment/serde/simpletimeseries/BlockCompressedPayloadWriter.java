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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockCompressedPayloadWriter
{
  private final ByteBuffer currentBlock;
  private final ByteBuffer compressedByteBuffer;
  private final BlockIndexWriter blockIndexWriter;
  private final WriteOutBytes dataOutBytes;
  private final Closer closer;
  private final CompressionStrategy.Compressor compressor;

  private boolean open = true;

  public BlockCompressedPayloadWriter(
      ByteBuffer currentBlock,
      ByteBuffer compressedByteBuffer,
      BlockIndexWriter blockIndexWriter,
      WriteOutBytes dataOutBytes,
      Closer closer,
      CompressionStrategy.Compressor compressor
  )
  {
    currentBlock.clear();
    compressedByteBuffer.clear();
    this.currentBlock = currentBlock;
    this.compressedByteBuffer = compressedByteBuffer;
    this.closer = closer;
    this.blockIndexWriter = blockIndexWriter;
    this.dataOutBytes = dataOutBytes;
    this.compressor = compressor;
  }

  public void write(byte[] payload) throws IOException
  {
    write(ByteBuffer.wrap(payload).order(ByteOrder.nativeOrder()));
  }

  public void write(ByteBuffer masterPayload) throws IOException
  {
    if (masterPayload == null) {
      return;
    }

    Preconditions.checkState(open, "cannot write to closed BlockCompressedPayloadWriter");
    ByteBuffer payload = masterPayload.asReadOnlyBuffer().order(masterPayload.order());

    while (payload.hasRemaining()) {
      int writeSize = Math.min(payload.remaining(), currentBlock.remaining());

      payload.limit(payload.position() + writeSize);
      currentBlock.put(payload);

      if (!currentBlock.hasRemaining()) {
        flush();
      }

      payload.limit(masterPayload.limit());
    }
  }

  public BlockCompressedPayloadSerializer close() throws IOException
  {
    if (open) {
      if (currentBlock.position() > 0) {
        flush();
      }

      blockIndexWriter.close();
      closer.close();
      open = false;
    }

    return new BlockCompressedPayloadSerializer(blockIndexWriter, dataOutBytes);
  }

  private void flush() throws IOException
  {
    Preconditions.checkState(open, "flush() on closed BlockCompressedPayloadWriter");
    currentBlock.flip();

    ByteBuffer actualCompressedByteBuffer = compressor.compress(currentBlock, compressedByteBuffer);
    int compressedBlockSize = actualCompressedByteBuffer.limit();

    blockIndexWriter.persistAndIncrement(compressedBlockSize);
    dataOutBytes.write(actualCompressedByteBuffer);
    currentBlock.clear();
    compressedByteBuffer.clear();
  }
}
