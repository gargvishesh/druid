/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadScribeToBytesWriter implements BytesWriter
{
  private final BlockCompressedPayloadScribe blockCompressedPayloadScribe;

  public BlockCompressedPayloadScribeToBytesWriter(BlockCompressedPayloadScribe blockCompressedPayloadScribe)
  {
    this.blockCompressedPayloadScribe = blockCompressedPayloadScribe;
  }

  @Override
  public void write(byte[] payload) throws IOException
  {
    blockCompressedPayloadScribe.write(payload);
  }

  @Override
  public void write(ByteBuffer payload) throws IOException
  {
    blockCompressedPayloadScribe.write(payload);
  }

  @Override
  public void close() throws IOException
  {
    blockCompressedPayloadScribe.close();
  }

  @Override
  public void transferTo(WritableByteChannel channel) throws IOException
  {
    blockCompressedPayloadScribe.writeTo(channel, null);
  }

  @Override
  public long getSerializedSize()
  {
    return blockCompressedPayloadScribe.getSerializedSize();
  }

  public static class Builder implements BytesWriterBuilder
  {
    private final BlockCompressedPayloadScribe.Builder builder;

    public Builder(BlockCompressedPayloadScribe.Builder builder)
    {
      this.builder = builder;
    }

    @Override
    public BytesWriter build() throws IOException
    {
      return new BlockCompressedPayloadScribeToBytesWriter(builder.build());
    }

    @Override
    public BytesWriterBuilder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      builder.setCompressionStrategy(compressionStrategy);

      return this;
    }
  }
}
