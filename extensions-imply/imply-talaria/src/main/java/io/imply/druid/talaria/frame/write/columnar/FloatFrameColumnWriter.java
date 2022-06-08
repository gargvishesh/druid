/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write.columnar;

import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.MemoryRange;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

public class FloatFrameColumnWriter implements FrameColumnWriter
{
  public static final long DATA_OFFSET = 1 /* type code */ + 1 /* has nulls? */;

  private final BaseFloatColumnValueSelector selector;
  private final AppendableMemory appendableMemory;
  private final boolean hasNulls;
  private final int sz;

  FloatFrameColumnWriter(
      BaseFloatColumnValueSelector selector,
      MemoryAllocator allocator,
      boolean hasNulls
  )
  {
    this.selector = selector;
    this.appendableMemory = AppendableMemory.create(allocator);
    this.hasNulls = hasNulls;
    this.sz = valueSize(hasNulls);
  }

  public static int valueSize(final boolean hasNulls)
  {
    return hasNulls ? Float.BYTES + 1 : Float.BYTES;
  }

  @Override
  public boolean addSelection()
  {
    if (!(appendableMemory.reserve(sz))) {
      return false;
    }

    final MemoryRange<WritableMemory> cursor = appendableMemory.cursor();
    final WritableMemory memory = cursor.memory();
    final long position = cursor.start();

    if (hasNulls) {
      if (selector.isNull()) {
        memory.putByte(position, (byte) 1);
        memory.putFloat(position + 1, 0);
      } else {
        memory.putByte(position, (byte) 0);
        memory.putFloat(position + 1, selector.getFloat());
      }
    } else {
      memory.putFloat(position, selector.getFloat());
    }

    appendableMemory.advanceCursor(sz);
    return true;
  }

  @Override
  public void undo()
  {
    appendableMemory.rewindCursor(sz);
  }

  @Override
  public long size()
  {
    return DATA_OFFSET + appendableMemory.size();
  }

  @Override
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    long currentPosition = startPosition;

    memory.putByte(currentPosition, FrameColumnWriters.TYPE_FLOAT);
    memory.putByte(currentPosition + 1, hasNulls ? (byte) 1 : (byte) 0);
    currentPosition += 2;

    currentPosition += appendableMemory.writeTo(memory, currentPosition);
    return currentPosition - startPosition;
  }

  @Override
  public void close()
  {
    appendableMemory.close();
  }
}
