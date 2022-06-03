/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RowReader
{

  private final RowIndexReader rowIndexReader;
  private final BlockCompressedPayloadReader dataReader;
  private final int serializedSize;

  private RowReader(RowIndexReader rowIndexReader, BlockCompressedPayloadReader dataReader, int serializedSize)
  {
    this.rowIndexReader = rowIndexReader;
    this.dataReader = dataReader;
    this.serializedSize = serializedSize;
  }

  public int getSerializedSize()
  {
    return serializedSize;
  }

  public ByteBuffer getRow(int rowNumber)
  {
    RowIndexReader.EntrySpan entrySpan = rowIndexReader.getEntrySpan(rowNumber);
    ByteBuffer payload = dataReader.read(entrySpan.getStart(), entrySpan.getSize());

    return payload;
  }

  public static class Factory
  {
    private final ByteBuffer rowIndexBuffer;
    private final ByteBuffer dataStorageBuffer;
    private final int serializedSize;

    public Factory(ByteBuffer originalByteBuffer)
    {
      ByteBuffer masterByteBuffer = originalByteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
      serializedSize = masterByteBuffer.limit();

      int rowIndexSize = masterByteBuffer.getInt();
      rowIndexBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      rowIndexBuffer.limit(rowIndexBuffer.position() + rowIndexSize);

      masterByteBuffer.position(masterByteBuffer.position() + rowIndexSize);

      int dataStorageSize = masterByteBuffer.getInt();
      dataStorageBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      dataStorageBuffer.limit(dataStorageBuffer.position() + dataStorageSize);
    }

    public RowReader create(ByteBuffer rowIndexUncompressedByteBuffer, ByteBuffer dataUncompressedByteBuffer)
    {
      RowIndexReader rowIndexReader = new RowIndexReader(BlockCompressedPayloadReader.create(
          rowIndexBuffer,
          rowIndexUncompressedByteBuffer
      ));
      BlockCompressedPayloadReader dataReader = BlockCompressedPayloadReader.create(
          dataStorageBuffer,
          dataUncompressedByteBuffer
      );
      RowReader rowReader = new RowReader(rowIndexReader, dataReader, serializedSize);

      return rowReader;
    }
  }
}
