/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class RowWriter
{
  private final IntSerializer intSerializer = new IntSerializer();
  private final RowIndexWriter rowIndexWriter;
  private final BlockCompressedPayloadScribe payloadScribe;

  public RowWriter(RowIndexWriter rowIndexWriter, BlockCompressedPayloadScribe payloadScribe)
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
    BlockCompressedPayloadScribe.Factory blockCompressedPayloadScribeFactory = new BlockCompressedPayloadScribe.Factory(
        new BlockCompressedPayloadWriterFactory(byteBufferProvider, segmentWriteOutMedium)
    );
    BlockCompressedPayloadScribe rowIndexScribe = blockCompressedPayloadScribeFactory.create();
    BlockCompressedPayloadScribe payloadScribe = blockCompressedPayloadScribeFactory.create();
    RowIndexWriter rowIndexWriter = new RowIndexWriter(rowIndexScribe);
    RowWriter rowWriter = new RowWriter(rowIndexWriter, payloadScribe);

    return rowWriter;
  }

  public void appendIndexedRow(byte[] rowBytes) throws IOException
  {
    payloadScribe.write(rowBytes);
    rowIndexWriter.persistAndIncrement(rowBytes.length);
  }

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    channel.write(intSerializer.serialize(rowIndexWriter.getSerializedSize()));
    rowIndexWriter.transferTo(channel);
    channel.write(intSerializer.serialize(payloadScribe.getSerializedSize()));
    payloadScribe.transferTo(channel);
  }

  public void close() throws IOException
  {
    rowIndexWriter.close();
    payloadScribe.close();
  }

  public long getSerializedSize()
  {
    return intSerializer.getSerializedSize()
           + rowIndexWriter.getSerializedSize()
           + intSerializer.getSerializedSize()
           + payloadScribe.getSerializedSize();
  }
}
