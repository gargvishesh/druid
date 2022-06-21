/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockCompressedPayloadWriterFactory
{
  private final NativeClearedByteBufferProvider byteBufferProvider;
  private final SegmentWriteOutMedium writeOutMedium;
  private final CompressionStrategy.Compressor compressor;

  public BlockCompressedPayloadWriterFactory(
      NativeClearedByteBufferProvider byteBufferProvider,
      SegmentWriteOutMedium writeOutMedium,
      CompressionStrategy.Compressor compressor
  )
  {
    this.byteBufferProvider = byteBufferProvider;
    this.writeOutMedium = writeOutMedium;
    this.compressor = compressor;
  }

  public BlockCompressedPayloadWriter create() throws IOException
  {
    Closer closer = Closer.create();
    ResourceHolder<ByteBuffer> currentBlockHolder = byteBufferProvider.get();

    closer.register(currentBlockHolder);

    ByteBuffer compressedBlockByteBuffer =
        CompressionStrategy.LZ4.getCompressor().allocateOutBuffer(currentBlockHolder.get().capacity(), closer);

    return new BlockCompressedPayloadWriter(
        currentBlockHolder.get(),
        compressedBlockByteBuffer,
        new BlockIndexWriter(writeOutMedium.makeWriteOutBytes()),
        writeOutMedium.makeWriteOutBytes(),
        closer,
        compressor
    );
  }
}
