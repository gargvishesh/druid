/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Comparator;

/**
 * Writer for a {@link FixedIndexed}
 */
public class FixedIndexedWriter<T> implements Serializer
{
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final TypeStrategy<T> typeStrategy;
  private final Comparator<T> comparator;
  private final ByteBuffer scratch;
  private final ByteBuffer readBuffer;
  private int numWritten;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private boolean hasNulls = false;
  private boolean isSorted;
  @Nullable
  private T prevObject = null;
  private final int width;

  public FixedIndexedWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      TypeStrategy<T> typeStrategy,
      ByteOrder byteOrder,
      int width,
      boolean isSorted
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.typeStrategy = typeStrategy;
    this.width = width;
    this.comparator = Comparator.nullsFirst(typeStrategy);
    this.scratch = ByteBuffer.allocate(width).order(byteOrder);
    this.readBuffer = ByteBuffer.allocate(width).order(byteOrder);
    this.isSorted = isSorted;
  }

  public void open() throws IOException
  {
    this.valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public long getSerializedSize()
  {
    return Byte.BYTES + Byte.BYTES + Integer.BYTES + valuesOut.size();
  }

  public void write(@Nullable T objectToWrite) throws IOException
  {
    if (prevObject != null && isSorted && comparator.compare(prevObject, objectToWrite) >= 0) {
      throw new ISE(
          "Values must be sorted and unique. Element [%s] with value [%s] is before or equivalent to [%s]",
          numWritten,
          objectToWrite,
          prevObject
      );
    }

    if (objectToWrite == null) {
      hasNulls = true;
      return;
    }

    scratch.clear();
    typeStrategy.write(scratch, objectToWrite, width);
    scratch.flip();
    Channels.writeFully(valuesOut, scratch);
    numWritten++;
    prevObject = objectToWrite;
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    scratch.clear();
    // version 0
    scratch.put((byte) 0);
    byte flags = 0x00;
    if (hasNulls) {
      flags = (byte) (flags | NullHandling.IS_NULL_BYTE);
    }
    if (isSorted) {
      flags = (byte) (flags | FixedIndexed.IS_SORTED_MASK);
    }
    scratch.put(flags);
    scratch.flip();
    Channels.writeFully(channel, scratch);
    scratch.clear();
    scratch.putInt(numWritten);
    scratch.flip();
    Channels.writeFully(channel, scratch);
    valuesOut.writeTo(channel);
  }

  @Nullable
  public T get(int index) throws IOException
  {
    if (index == 0 && hasNulls) {
      return null;
    }
    int startOffset = index * width;
    readBuffer.clear();
    valuesOut.readFully(startOffset, readBuffer);
    readBuffer.clear();
    return typeStrategy.read(readBuffer);
  }
}
