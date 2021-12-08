/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.MemoryWithRange;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.io.Channels;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

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

    final MemoryWithRange<WritableMemory> cursor = appendableMemory.cursor();
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
  public int compare(int row1, int row2)
  {
    final long position1 = (long) row1 * sz;
    final long position2 = (long) row2 * sz;

    if (hasNulls) {
      final MemoryWithRange<WritableMemory> cursor1 = appendableMemory.read(position1);
      final MemoryWithRange<WritableMemory> cursor2 = appendableMemory.read(position2);

      final WritableMemory region1 = cursor1.memory();
      final WritableMemory region2 = cursor2.memory();

      final long regionPosition1 = cursor1.start();
      final long regionPosition2 = cursor2.start();

      // Nulls first
      int cmp = Byte.compare(region2.getByte(regionPosition2), region1.getByte(regionPosition1));

      if (cmp != 0) {
        return cmp;
      }

      return Float.compare(region1.getFloat(regionPosition1 + 1), region2.getFloat(regionPosition2 + 1));
    } else {
      return Float.compare(appendableMemory.getFloat(position1), appendableMemory.getFloat(position2));
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    final ByteBuffer buf = ByteBuffer.allocate(Ints.checkedCast(DATA_OFFSET)).order(ByteOrder.nativeOrder());
    buf.put(FrameColumnWriters.TYPE_FLOAT).put(hasNulls ? (byte) 1 : (byte) 0).flip();
    Channels.writeFully(channel, buf);
    appendableMemory.writeTo(channel);
  }

  @Override
  public void close()
  {
    appendableMemory.close();
  }
}
