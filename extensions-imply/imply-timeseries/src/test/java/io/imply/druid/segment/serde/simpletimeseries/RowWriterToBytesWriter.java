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

public class RowWriterToBytesWriter implements BytesWriter
{
  private final RowWriter rowWriter;

  public RowWriterToBytesWriter(RowWriter rowWriter)
  {
    this.rowWriter = rowWriter;
  }

  @Override
  public void write(byte[] rowBytes) throws IOException
  {
    rowWriter.write(rowBytes);
  }

  @Override
  public void write(ByteBuffer rowByteBuffer) throws IOException
  {
    rowWriter.write(rowByteBuffer);
  }

  @Override
  public void transferTo(WritableByteChannel channel) throws IOException
  {
    rowWriter.writeTo(channel, null);
  }

  @Override
  public void close() throws IOException
  {
    rowWriter.close();
  }

  @Override
  public long getSerializedSize()
  {
    return rowWriter.getSerializedSize();
  }

  public static class Builder implements BytesWriterBuilder
  {
    private final RowWriter.Builder builder;

    public Builder(RowWriter.Builder builder)
    {
      this.builder = builder;
    }

    @Override
    public BytesWriterBuilder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      builder.setCompressionStrategy(compressionStrategy);

      return this;
    }

    @Override
    public BytesWriter build() throws IOException
    {
      return new RowWriterToBytesWriter(builder.build());
    }
  }
}
