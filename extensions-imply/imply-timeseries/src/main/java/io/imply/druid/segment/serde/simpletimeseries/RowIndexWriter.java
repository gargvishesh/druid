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
import org.apache.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;


public class RowIndexWriter implements Serializer
{
  private final LongSerializer longSerializer = new LongSerializer();
  private final BlockCompressedPayloadScribe payloadScribe;

  private long position = 0;
  private boolean open = true;

  public RowIndexWriter(BlockCompressedPayloadScribe payloadScribe)
  {
    this.payloadScribe = payloadScribe;
  }

  public void persistAndIncrement(int increment) throws IOException
  {
    Preconditions.checkArgument(increment >= 0);
    Preconditions.checkState(open, "cannot write to closed RowIndex");
    payloadScribe.write(longSerializer.serialize(position));
    position += increment;
  }

  public void close() throws IOException
  {
    Preconditions.checkState(open, "cannot close a closed RowIndex");
    payloadScribe.write(longSerializer.serialize(position));
    payloadScribe.close();
    open = false;
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    payloadScribe.writeTo(channel, smoosher);
  }

  @Override
  public long getSerializedSize()
  {
    return payloadScribe.getSerializedSize();
  }
}
