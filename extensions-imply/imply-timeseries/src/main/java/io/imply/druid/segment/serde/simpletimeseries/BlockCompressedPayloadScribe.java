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
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadScribe implements Serializer
{
  private final BlockCompressedPayloadWriterFactory writerFactory;
  private BlockCompressedPayloadWriter writer;
  private BlockCompressedPayloadSerializer serializer;
  private State state = State.START;

  private BlockCompressedPayloadScribe(BlockCompressedPayloadWriterFactory writerFactory)
  {
    this.writerFactory = writerFactory;
  }

  public void open() throws IOException
  {
    Preconditions.checkState(state == State.START || state == State.OPEN);

    if (state == State.START) {
      writer = writerFactory.create();
      state = State.OPEN;
    }
  }

  public void write(byte[] payload) throws IOException
  {
    Preconditions.checkState(state == State.OPEN);
    writer.write(payload);
  }

  public void write(ByteBuffer payload) throws IOException
  {
    Preconditions.checkState(state == State.OPEN);
    writer.write(payload);
  }


  public void close() throws IOException
  {
    if (state == State.OPEN) {
      serializer = writer.close();
      writer = null;
      state = State.CLOSED;
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(state == State.CLOSED);
    serializer.writeTo(channel, smoosher);
  }

  @Override
  public long getSerializedSize()
  {
    Preconditions.checkState(state == State.CLOSED);
    return serializer.getSerializedSize();
  }

  private enum State
  {
    START,
    OPEN,
    CLOSED
  }

  public static class Builder
  {
    private final NativeClearedByteBufferProvider byteBufferProvider;
    private final SegmentWriteOutMedium writeOutMedium;

    private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;

    public Builder(NativeClearedByteBufferProvider byteBufferProvider, SegmentWriteOutMedium writeOutMedium)
    {

      this.byteBufferProvider = byteBufferProvider;
      this.writeOutMedium = writeOutMedium;
    }

    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      this.compressionStrategy = compressionStrategy;

      return this;
    }

    public BlockCompressedPayloadScribe build() throws IOException
    {
      BlockCompressedPayloadScribe payloadScribe =
          new BlockCompressedPayloadScribe(new BlockCompressedPayloadWriterFactory(
              byteBufferProvider,
              writeOutMedium,
              compressionStrategy.getCompressor()
          ));

      payloadScribe.open();

      return payloadScribe;
    }
  }
}
