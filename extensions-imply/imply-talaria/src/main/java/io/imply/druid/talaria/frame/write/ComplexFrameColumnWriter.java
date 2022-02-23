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
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Column writer for complex columns.
 *
 * Dual to {@link io.imply.druid.talaria.frame.read.ComplexFrameColumnReader}.
 */
public class ComplexFrameColumnWriter implements FrameColumnWriter
{
  // Less than half of AppendableMemory.DEFAULT_INITIAL_ALLOCATION_SIZE.
  // This guarantees we can fit a WorkerMemoryParmeters.MAX_FRAME_COLUMNS number of columns into a frame.
  private static final int INITIAL_ALLOCATION_SIZE = 128;

  public static final byte NOT_NULL_MARKER = 0x00;
  public static final byte NULL_MARKER = 0x01;

  private final ComplexMetricSerde serde;
  private final BaseObjectColumnValueSelector<?> selector;
  private final AppendableMemory offsetMemory;
  private final AppendableMemory dataMemory;
  private final byte[] typeNameBytes;

  private int lastDataLength = -1;

  ComplexFrameColumnWriter(
      final BaseObjectColumnValueSelector<?> selector,
      final MemoryAllocator allocator,
      final ComplexMetricSerde serde
  )
  {
    this.selector = selector;
    this.serde = serde;
    this.offsetMemory = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.dataMemory = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.typeNameBytes = StringUtils.toUtf8(serde.getTypeName());
  }

  @Override
  public boolean addSelection()
  {
    if (!offsetMemory.reserve(Integer.BYTES)) {
      return false;
    }

    final Object complexObject = selector.getObject();
    final byte[] complexBytes = complexObject == null ? ByteArrays.EMPTY_ARRAY : serde.toBytes(complexObject);

    if (complexBytes.length == Integer.MAX_VALUE) {
      // Cannot handle objects this large.
      return false;
    }

    final int dataLength = complexBytes.length + 1;

    if (dataMemory.size() + dataLength > Integer.MAX_VALUE || !(dataMemory.reserve(dataLength))) {
      return false;
    }

    // All space is reserved. Start writing.
    final MemoryWithRange<WritableMemory> offsetCursor = offsetMemory.cursor();
    offsetCursor.memory().putInt(offsetCursor.start(), Ints.checkedCast(dataMemory.size() + dataLength));
    offsetMemory.advanceCursor(Integer.BYTES);

    final MemoryWithRange<WritableMemory> dataCursor = dataMemory.cursor();
    dataCursor.memory().putByte(dataCursor.start(), complexObject == null ? NULL_MARKER : NOT_NULL_MARKER);
    dataCursor.memory().putByteArray(dataCursor.start() + 1, complexBytes, 0, complexBytes.length);
    dataMemory.advanceCursor(dataLength);

    lastDataLength = dataLength;
    return true;
  }

  @Override
  public void undo()
  {
    if (lastDataLength == -1) {
      throw new ISE("Nothing to undo");
    }

    offsetMemory.rewindCursor(Integer.BYTES);
    dataMemory.rewindCursor(lastDataLength);
    lastDataLength = -1;
  }

  @Override
  public int compare(int row1, int row2)
  {
    // Complex columns cannot be sorted.
    throw new UnsupportedOperationException();
  }

  @Override
  public long size()
  {
    return headerSize() + offsetMemory.size() + dataMemory.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    final ByteBuffer buf = ByteBuffer.allocate(headerSize()).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(FrameColumnWriters.TYPE_COMPLEX)
       .putInt(typeNameBytes.length)
       .put(typeNameBytes)
       .flip();
    Channels.writeFully(channel, buf);
    offsetMemory.writeTo(channel);
    dataMemory.writeTo(channel);
  }

  @Override
  public void close()
  {
    offsetMemory.close();
    dataMemory.close();
  }

  private int headerSize()
  {
    return 1 /* type code */
           + Integer.BYTES /* type name length */
           + typeNameBytes.length;
  }
}
