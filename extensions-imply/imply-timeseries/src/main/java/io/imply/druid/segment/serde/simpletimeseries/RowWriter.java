/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class RowWriter implements Serializer
{
  private final IntSerializer intSerializer = new IntSerializer();
  private final RowIndexWriter rowIndexWriter;
  private final BlockCompressedPayloadScribe payloadScribe;

  private RowWriter(RowIndexWriter rowIndexWriter, BlockCompressedPayloadScribe payloadScribe)
  {
    this.rowIndexWriter = rowIndexWriter;
    this.payloadScribe = payloadScribe;
  }

  public static RowWriter create(
      NativeClearedByteBufferProvider byteBufferProvider,
      SegmentWriteOutMedium segmentWriteOutMedium
  )
      throws IOException
  {
    BlockCompressedPayloadScribe.Builder blockCompressedPayloadScribeBuilder =
        new BlockCompressedPayloadScribe.Builder(byteBufferProvider, segmentWriteOutMedium);
    BlockCompressedPayloadScribe rowIndexScribe = blockCompressedPayloadScribeBuilder.build();
    BlockCompressedPayloadScribe payloadScribe = blockCompressedPayloadScribeBuilder.build();
    RowIndexWriter rowIndexWriter = new RowIndexWriter(rowIndexScribe);
    RowWriter rowWriter = new RowWriter(rowIndexWriter, payloadScribe);

    return rowWriter;
  }

  public void write(byte[] rowBytes) throws IOException
  {
    payloadScribe.write(rowBytes);
    rowIndexWriter.persistAndIncrement(rowBytes.length);
  }

  public void write(ByteBuffer rowByteBuffer) throws IOException
  {
    int size = rowByteBuffer == null ? 0 : rowByteBuffer.remaining();
    payloadScribe.write(rowByteBuffer);
    rowIndexWriter.persistAndIncrement(size);
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    channel.write(intSerializer.serialize(rowIndexWriter.getSerializedSize()));
    rowIndexWriter.writeTo(channel, smoosher);
    channel.write(intSerializer.serialize(payloadScribe.getSerializedSize()));
    payloadScribe.writeTo(channel, smoosher);
  }

  public void close() throws IOException
  {
    rowIndexWriter.close();
    payloadScribe.close();
  }

  @Override
  public long getSerializedSize()
  {
    return intSerializer.getSerializedSize()
           + rowIndexWriter.getSerializedSize()
           + intSerializer.getSerializedSize()
           + payloadScribe.getSerializedSize();
  }

  public static class Builder
  {
    private final BlockCompressedPayloadScribe.Builder blockCompressedPayloadScribeBuilder;

    public Builder(NativeClearedByteBufferProvider byteBufferProvider, SegmentWriteOutMedium segmentWriteOutMedium)
    {
      blockCompressedPayloadScribeBuilder =
          new BlockCompressedPayloadScribe.Builder(byteBufferProvider, segmentWriteOutMedium);
    }

    public Builder(SegmentWriteOutMedium segmentWriteOutMedium)
    {
      this(NativeClearedByteBufferProvider.DEFAULT, segmentWriteOutMedium);
    }

    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      blockCompressedPayloadScribeBuilder.setCompressionStrategy(compressionStrategy);

      return this;
    }

    public RowWriter build() throws IOException
    {
      BlockCompressedPayloadScribe rowIndexScribe = blockCompressedPayloadScribeBuilder.build();
      BlockCompressedPayloadScribe payloadScribe = blockCompressedPayloadScribeBuilder.build();
      RowIndexWriter rowIndexWriter = new RowIndexWriter(rowIndexScribe);

      return new RowWriter(rowIndexWriter, payloadScribe);
    }
  }
}
