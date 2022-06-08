/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write.columnar;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.MemoryRange;
import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class StringFrameColumnWriter<T extends ColumnValueSelector> implements FrameColumnWriter
{
  // Multiple of 4 such that three of these fit within AppendableMemory.DEFAULT_INITIAL_ALLOCATION_SIZE.
  // This guarantees we can fit a WorkerMemoryParmeters.MAX_FRAME_COLUMNS number of columns into a frame.
  private static final int INITIAL_ALLOCATION_SIZE = 120;

  public static final long DATA_OFFSET = 1 /* type code */ + 1 /* single or multi-value? */;

  private final T selector;
  protected final boolean multiValue;

  // Row lengths: one int per row with the number of values contained by that row and all previous rows.
  // Only written for multi-value columns.
  private final AppendableMemory cumulativeRowLengths;

  // String lengths: one int per string, containing the length of that string plus the length of all previous strings.
  private final AppendableMemory cumulativeStringLengths;

  // String data.
  private final AppendableMemory stringData;

  private int lastCumulativeRowLength = 0;
  private int lastRowLength = 0;
  private int lastCumulativeStringLength = 0;
  private int lastStringLength = -1;

  StringFrameColumnWriter(
      final T selector,
      final MemoryAllocator allocator,
      final boolean multiValue
  )
  {
    this.selector = selector;
    this.multiValue = multiValue;

    if (multiValue) {
      this.cumulativeRowLengths = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    } else {
      this.cumulativeRowLengths = null;
    }

    this.cumulativeStringLengths = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.stringData = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
  }

  @Override
  public boolean addSelection()
  {
    // TODO(gianm): retain dictionary codes from selectors

    final List<ByteBuffer> utf8Data = getUtf8ByteBuffersFromSelector(selector);
    final int utf8DataByteLength = countBytes(utf8Data);

    if ((long) lastCumulativeRowLength + utf8Data.size() > Integer.MAX_VALUE) {
      // Column is full because cumulative row length has exceeded the max capacity of an integer.
      return false;
    }

    if ((long) lastCumulativeStringLength + utf8DataByteLength > Integer.MAX_VALUE) {
      // Column is full because cumulative string length has exceeded the max capacity of an integer.
      return false;
    }

    if (multiValue && !cumulativeRowLengths.reserve(Integer.BYTES)) {
      return false;
    }

    if (!cumulativeStringLengths.reserve(Integer.BYTES * utf8Data.size())) {
      return false;
    }

    if (!stringData.reserve(utf8DataByteLength)) {
      return false;
    }

    // Enough space has been reserved to write what we need to write; let's start.
    if (multiValue) {
      final MemoryRange<WritableMemory> rowLengthsCursor = cumulativeRowLengths.cursor();
      rowLengthsCursor.memory().putInt(rowLengthsCursor.start(), lastCumulativeRowLength + utf8Data.size());
      cumulativeRowLengths.advanceCursor(Integer.BYTES);
      lastRowLength = utf8Data.size();
      lastCumulativeRowLength += utf8Data.size();
    }

    // The utf8Data.size and utf8DataByteLength checks are necessary to avoid acquiring cursors with zero bytes
    // reserved. Otherwise, if a zero-byte-reserved cursor was acquired in the first row, it would be an error since no
    // bytes would have been allocated yet.
    final MemoryRange<WritableMemory> stringLengthsCursor =
        utf8Data.size() > 0 ? cumulativeStringLengths.cursor() : null;
    final MemoryRange<WritableMemory> stringDataCursor =
        utf8DataByteLength > 0 ? stringData.cursor() : null;

    lastStringLength = 0;
    for (int i = 0; i < utf8Data.size(); i++) {
      final ByteBuffer utf8Datum = utf8Data.get(i);
      final int len = utf8Datum.remaining();

      if (len > 0) {
        FrameWriterUtils.copyByteBufferToMemory(
            utf8Datum,
            stringDataCursor.memory(),
            stringDataCursor.start() + lastStringLength,
            len,
            true
        );
      }

      lastStringLength += len;
      lastCumulativeStringLength += len;
      stringLengthsCursor.memory()
                         .putInt(stringLengthsCursor.start() + (long) Integer.BYTES * i, lastCumulativeStringLength);
    }

    if (utf8Data.size() > 0) {
      cumulativeStringLengths.advanceCursor(Integer.BYTES * utf8Data.size());
    }

    if (utf8DataByteLength > 0) {
      stringData.advanceCursor(lastStringLength);
    }

    return true;
  }

  @Override
  public void undo()
  {
    if (lastStringLength == -1) {
      throw new ISE("Cannot undo");
    }

    if (multiValue) {
      cumulativeRowLengths.rewindCursor(Integer.BYTES);
      cumulativeStringLengths.rewindCursor(Integer.BYTES * lastRowLength);
      lastCumulativeRowLength -= lastRowLength;
      lastRowLength = 0;
    } else {
      cumulativeStringLengths.rewindCursor(Integer.BYTES);
    }

    stringData.rewindCursor(lastStringLength);
    lastCumulativeStringLength -= lastStringLength;
    lastStringLength = -1; // Sigil value that allows detection of incorrect "undo" calls
  }

  @Override
  public long size()
  {
    return DATA_OFFSET
           + (multiValue ? cumulativeRowLengths.size() : 0)
           + cumulativeStringLengths.size()
           + stringData.size();
  }

  @Override
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    long currentPosition = startPosition;

    memory.putByte(currentPosition, FrameColumnWriters.TYPE_STRING);
    memory.putByte(currentPosition + 1, multiValue ? (byte) 1 : (byte) 0);
    currentPosition += 2;

    if (multiValue) {
      currentPosition += cumulativeRowLengths.writeTo(memory, currentPosition);
    }

    currentPosition += cumulativeStringLengths.writeTo(memory, currentPosition);
    currentPosition += stringData.writeTo(memory, currentPosition);

    return currentPosition - startPosition;
  }

  @Override
  public void close()
  {
    if (multiValue) {
      cumulativeRowLengths.close();
    }

    cumulativeStringLengths.close();
    stringData.close();
  }

  /**
   * Extracts a list of ByteBuffers from the selector. Null values are returned as
   * {@link FrameWriterUtils#NULL_STRING_MARKER_ARRAY}.
   */
  public abstract List<ByteBuffer> getUtf8ByteBuffersFromSelector(T selector);

  /**
   * Returns the sum of remaining bytes in the provided list of byte buffers.
   */
  private static int countBytes(final List<ByteBuffer> buffers)
  {
    long count = 0;

    for (final ByteBuffer buffer : buffers) {
      count += buffer.remaining();
    }

    // Hopefully there's never more than 2GB of string per row!
    return Ints.checkedCast(count);
  }
}

class StringFrameColumnWriterImpl extends StringFrameColumnWriter<DimensionSelector>
{
  StringFrameColumnWriterImpl(
      DimensionSelector selector,
      MemoryAllocator allocator,
      boolean multiValue
  )
  {
    super(selector, allocator, multiValue);
  }

  @Override
  public List<ByteBuffer> getUtf8ByteBuffersFromSelector(final DimensionSelector selector)
  {
    return FrameWriterUtils.getUtf8ByteBuffersFromStringSelector(selector, multiValue);
  }
}

class StringArrayFrameColumnWriter extends StringFrameColumnWriter<ColumnValueSelector>
{
  StringArrayFrameColumnWriter(
      ColumnValueSelector selector,
      MemoryAllocator allocator,
      boolean multiValue
  )
  {
    super(selector, allocator, multiValue);
    Preconditions.checkArgument(
        multiValue,
        "%s can only be used when multiValue is true",
        StringArrayFrameColumnWriter.class.getName()
    );
  }

  @Override
  public List<ByteBuffer> getUtf8ByteBuffersFromSelector(final ColumnValueSelector selector)
  {
    return FrameWriterUtils.getUtf8ByteBuffersFromStringArraySelector(selector);
  }
}
