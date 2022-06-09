/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class CompressedVariableSizedBlobColumn implements Closeable
{
  private final int numElements;
  private final CompressedLongsReader compressedOffsets;
  private final CompressedBlockReader compressedBlocks;
  private final Closer closer;

  public CompressedVariableSizedBlobColumn(
      int numElements,
      CompressedLongsReader offsetsReader,
      CompressedBlockReader blockReader
  )
  {
    this.numElements = numElements;
    this.compressedOffsets = offsetsReader;
    this.compressedBlocks = blockReader;
    this.closer = Closer.create();
    closer.register(compressedOffsets);
    closer.register(compressedBlocks);
  }

  public int size()
  {
    return numElements;
  }

  public ByteBuffer get(int index)
  {
    final long startOffset;
    final long endOffset;

    if (index == 0) {
      startOffset = 0;
      endOffset = compressedOffsets.get(0);
    } else {
      startOffset = compressedOffsets.get(index - 1);
      endOffset = compressedOffsets.get(index);
    }
    final int size = Ints.checkedCast(endOffset - startOffset);
    return compressedBlocks.getRange(startOffset, size);
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }
}
