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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadScribe
{
  private final BlockCompressedPayloadWriterFactory writerFactory;
  private BlockCompressedPayloadWriter writer;
  private BlockCompressedPayloadSerializer serializer;
  private State state = State.START;

  public BlockCompressedPayloadScribe(BlockCompressedPayloadWriterFactory writerFactory)
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

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    Preconditions.checkState(state == State.CLOSED);
    serializer.transferTo(channel);
  }

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

  public static class Factory
  {
    private final BlockCompressedPayloadWriterFactory writerFactory;

    public Factory(BlockCompressedPayloadWriterFactory writerFactory)
    {
      this.writerFactory = writerFactory;
    }

    public BlockCompressedPayloadScribe create() throws IOException
    {
      BlockCompressedPayloadScribe payloadScribe = new BlockCompressedPayloadScribe(writerFactory);

      payloadScribe.open();

      return payloadScribe;
    }
  }
}
