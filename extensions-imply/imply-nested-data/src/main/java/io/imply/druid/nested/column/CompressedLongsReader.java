/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.utils.CloseableUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

public final class CompressedLongsReader implements ColumnarLongs
{
  public static Supplier<CompressedLongsReader> fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    final Supplier<CompressedBlockReader> baseReader = CompressedBlockReader.fromByteBuffer(buffer, order);
    return () -> new CompressedLongsReader(baseReader.get());
  }

  private final CompressedBlockReader blockReader;
  private final ByteBuffer buffer;

  public CompressedLongsReader(CompressedBlockReader blockReader)
  {
    this.blockReader = blockReader;
    this.buffer = blockReader.getDecompressedDataBuffer().asReadOnlyBuffer()
                             .order(blockReader.getDecompressedDataBuffer().order());
  }

  @Override
  public long get(int index)
  {
    final long byteOffset = (long) index * Long.BYTES;
    final int offset = blockReader.loadBlock(byteOffset);
    return buffer.getLong(offset);
  }

  @Override
  public int size()
  {
    return (int) (blockReader.getSize() / Long.BYTES);
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(blockReader);
  }
}
