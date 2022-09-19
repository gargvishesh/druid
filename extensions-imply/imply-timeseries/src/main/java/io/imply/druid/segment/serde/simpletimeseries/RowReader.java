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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RowReader
{
  private final RowIndexReader rowIndexReader;
  private final BlockCompressedPayloadReader dataReader;

  private RowReader(RowIndexReader rowIndexReader, BlockCompressedPayloadReader dataReader)
  {
    this.rowIndexReader = rowIndexReader;
    this.dataReader = dataReader;
  }

  public ByteBuffer getRow(int rowNumber)
  {
    PayloadEntrySpan payloadEntrySpan = rowIndexReader.getEntrySpan(rowNumber);
    ByteBuffer payload = dataReader.read(payloadEntrySpan.getStart(), payloadEntrySpan.getSize());

    return payload;
  }

  public static class Builder
  {
    private final ByteBuffer rowIndexBuffer;
    private final ByteBuffer dataStorageBuffer;

    private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;

    public Builder(ByteBuffer originalByteBuffer)
    {
      ByteBuffer masterByteBuffer = originalByteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

      int rowIndexSize = masterByteBuffer.getInt();
      rowIndexBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      rowIndexBuffer.limit(rowIndexBuffer.position() + rowIndexSize);

      masterByteBuffer.position(masterByteBuffer.position() + rowIndexSize);

      int dataStorageSize = masterByteBuffer.getInt();
      dataStorageBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      dataStorageBuffer.limit(dataStorageBuffer.position() + dataStorageSize);
    }

    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      this.compressionStrategy = compressionStrategy;

      return this;
    }

    public RowReader build(ByteBuffer rowIndexUncompressedByteBuffer, ByteBuffer dataUncompressedByteBuffer)
    {
      RowIndexReader rowIndexReader = new RowIndexReader(BlockCompressedPayloadReader.create(
          rowIndexBuffer,
          rowIndexUncompressedByteBuffer,
          compressionStrategy.getDecompressor()
      ));
      BlockCompressedPayloadReader dataReader = BlockCompressedPayloadReader.create(
          dataStorageBuffer,
          dataUncompressedByteBuffer,
          compressionStrategy.getDecompressor()
      );
      RowReader rowReader = new RowReader(rowIndexReader, dataReader);

      return rowReader;
    }
  }
}
