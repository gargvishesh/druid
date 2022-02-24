/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.MemoryWithRange;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class StringFrameColumnWriter<T extends ColumnValueSelector> implements FrameColumnWriter
{
  // Multiple of 4 such that three of these fit within AppendableMemory.DEFAULT_INITIAL_ALLOCATION_SIZE.
  // This guarantees we can fit a WorkerMemoryParmeters.MAX_FRAME_COLUMNS number of columns into a frame.
  private static final int INITIAL_ALLOCATION_SIZE = 120;

  public static final long DATA_OFFSET = 1 /* type code */ + 1 /* single or multi-value? */;
  public static final byte NULL_MARKER = (byte) 0xFF; /* cannot appear in a valid utf-8 byte sequence */

  protected static final byte[] NULL_MARKER_ARRAY = new byte[]{NULL_MARKER};

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
      final MemoryWithRange<WritableMemory> rowLengthsCursor = cumulativeRowLengths.cursor();
      rowLengthsCursor.memory().putInt(rowLengthsCursor.start(), lastCumulativeRowLength + utf8Data.size());
      cumulativeRowLengths.advanceCursor(Integer.BYTES);
      lastRowLength = utf8Data.size();
      lastCumulativeRowLength += utf8Data.size();
    }

    // The utf8Data.size and utf8DataByteLength checks are necessary to avoid acquiring cursors with zero bytes
    // reserved. Otherwise, if a zero-byte-reserved cursor was acquired in the first row, it would be an error since no
    // bytes would have been allocated yet.
    final MemoryWithRange<WritableMemory> stringLengthsCursor =
        utf8Data.size() > 0 ? cumulativeStringLengths.cursor() : null;
    final MemoryWithRange<WritableMemory> stringDataCursor =
        utf8DataByteLength > 0 ? stringData.cursor() : null;

    lastStringLength = 0;
    for (int i = 0; i < utf8Data.size(); i++) {
      final ByteBuffer utf8Datum = utf8Data.get(i);
      final int len = utf8Datum.remaining();

      if (len > 0) {
        Memory.wrap(utf8Datum).copyTo(
            utf8Datum.position(),
            stringDataCursor.memory(),
            stringDataCursor.start() + lastStringLength,
            len
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
  public int compare(final int row1, final int row2)
  {
    // TODO(gianm): add an additional level of indirection when we can retain dict codes; enables comparison by dict codes

    if (multiValue) {
      // Compare multi-value string columns like StringDimensionHandler.DIMENSION_SELECTOR_COMPARATOR.
      // Except: don't treat null and empty array as the same, because DIMENSION_SELECTOR_COMPARATOR only does that
      // so it can compare values from different columns in a way that works out for indexing. This method does
      // not have that concern, since it's comparing two rows from the same column.
      final IntIntPair rowBounds1 = getRowBounds(row1);
      final IntIntPair rowBounds2 = getRowBounds(row2);

      for (int i = rowBounds1.firstInt(), j = rowBounds2.firstInt();
           i < rowBounds1.secondInt() && j < rowBounds2.secondInt();
           i++, j++) {
        final int cmp = compareStringDataNullsFirst(i, j);
        if (cmp != 0) {
          return cmp;
        }
      }

      return Integer.compare(
          rowBounds1.secondInt() - rowBounds1.firstInt(),
          rowBounds2.secondInt() - rowBounds2.firstInt()
      );
    } else {
      return compareStringDataNullsFirst(row1, row2);
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    final ByteBuffer buf = ByteBuffer.allocate(Ints.checkedCast(DATA_OFFSET)).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(FrameColumnWriters.TYPE_STRING).put(multiValue ? (byte) 1 : (byte) 0).flip();
    Channels.writeFully(channel, buf);
    if (multiValue) {
      cumulativeRowLengths.writeTo(channel);
    }
    cumulativeStringLengths.writeTo(channel);
    stringData.writeTo(channel);
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
   * Retrieves the start (inclusive) and end (exclusive) string numbers for a particular row that has already been
   * written. Used by {@link #compare(int, int)}. Only valid for multi-value columns.
   */
  private IntIntPair getRowBounds(final int row)
  {
    if (!multiValue) {
      throw new ISE("Column is not multi-value");
    }

    final int end = cumulativeRowLengths.getInt((long) Integer.BYTES * row);

    final int start;

    if (row == 0) {
      start = 0;
    } else {
      start = cumulativeRowLengths.getInt((long) Integer.BYTES * (row - 1));
    }

    return IntIntPair.of(start, end);
  }

  /**
   * Retrieves string data that has already been written. Used by {@link #compare(int, int)}.
   */
  @Nullable
  private MemoryWithRange<Memory> getUtf8StringData(final int stringNumber)
  {

    final int stringStart;
    final int stringEnd = cumulativeStringLengths.getInt((long) Integer.BYTES * stringNumber);

    if (stringNumber == 0) {
      stringStart = 0;
    } else {
      stringStart = cumulativeStringLengths.getInt((long) Integer.BYTES * (stringNumber - 1));
    }

    final MemoryWithRange<WritableMemory> dataCursor = stringData.read(stringStart);
    final int stringLength = stringEnd - stringStart;

    if (stringLength == 1 && dataCursor.memory().getByte(dataCursor.start()) == NULL_MARKER) {
      return null;
    } else {
      return new MemoryWithRange<>(
          dataCursor.memory(),
          dataCursor.start(),
          dataCursor.start() + stringLength
      );
    }
  }

  /**
   * Extracts a list of ByteBuffers from the selector. Null values are returned as {@link #NULL_MARKER_ARRAY}.
   */
  public abstract List<ByteBuffer> getUtf8ByteBuffersFromSelector(
      T selector
  );

  /**
   * Extracts a ByteBuffer from the string. Null values are returned as {@link #NULL_MARKER_ARRAY}.
   */
  protected static ByteBuffer getUtf8ByteBufferFromString(final String data)
  {
    if (NullHandling.isNullOrEquivalent(data)) {
      return ByteBuffer.wrap(NULL_MARKER_ARRAY);
    } else {
      return ByteBuffer.wrap(StringUtils.toUtf8(data));
    }
  }

  private int compareStringDataNullsFirst(final int stringNumber1, final int stringNumber2)
  {
    final MemoryWithRange<Memory> memoryRange1 = getUtf8StringData(stringNumber1);
    final MemoryWithRange<Memory> memoryRange2 = getUtf8StringData(stringNumber2);

    if (memoryRange1 == null) {
      // Nulls first.
      return memoryRange2 == null ? 0 : -1;
    } else if (memoryRange2 == null) {
      // Nulls first.
      return 1;
    } else {
      // Avoid memory.compareTo, because it compares using signed bytes, which gives the wrong ordering.
      final long commonLength = Math.min(memoryRange1.length(), memoryRange2.length());
      final Memory memory1 = memoryRange1.memory();
      final Memory memory2 = memoryRange2.memory();
      final long start1 = memoryRange1.start();
      final long start2 = memoryRange2.start();

      for (long i = 0; i < commonLength; i++) {
        final byte byte1 = memory1.getByte(start1 + i);
        final byte byte2 = memory2.getByte(start2 + i);
        final int cmp = (byte1 & 0xFF) - (byte2 & 0xFF); // Unsigned comparison
        if (cmp != 0) {
          return cmp;
        }
      }

      return Long.compare(memoryRange1.length(), memoryRange2.length());
    }
  }

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
    final IndexedInts row = selector.getRow();
    final int size = row.size();

    if (multiValue) {
      final List<ByteBuffer> retVal = new ArrayList<>(size);

      for (int i = 0; i < size; i++) {
        retVal.add(getUtf8ByteBufferFromRowIndex(selector, row.get(i)));
      }

      return retVal;
    } else {
      // If !multivalue, always return exactly one buffer.
      if (size == 0) {
        return Collections.singletonList(ByteBuffer.wrap(NULL_MARKER_ARRAY));
      } else {
        return Collections.singletonList(getUtf8ByteBufferFromRowIndex(selector, row.get(0)));
      }
    }
  }

  private ByteBuffer getUtf8ByteBufferFromRowIndex(final DimensionDictionarySelector selector, final int index)
  {
    if (selector.supportsLookupNameUtf8()) {
      final ByteBuffer buf = selector.lookupNameUtf8(index);

      if (buf == null || (NullHandling.replaceWithDefault() && buf.remaining() == 0)) {
        return ByteBuffer.wrap(NULL_MARKER_ARRAY);
      } else {
        return buf;
      }
    } else {
      return getUtf8ByteBufferFromString(selector.lookupName(index));
    }
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
    Object row = selector.getObject();
    if (row == null) {
      return Collections.singletonList(getUtf8ByteBufferFromString(null));
    } else if (row instanceof String) {
      return Collections.singletonList(getUtf8ByteBufferFromString((String) row));
    }

    final List<ByteBuffer> retVal = new ArrayList<>();
    if (row instanceof List) {
      for (int i = 0; i < ((List<?>) row).size(); i++) {
        retVal.add(getUtf8ByteBufferFromString(((List<String>) row).get(i)));
      }
    } else if (row instanceof String[]) {
      for (String value : (String[]) row) {
        retVal.add(getUtf8ByteBufferFromString(value));
      }
    } else if (row instanceof ComparableStringArray) {
      for (String value : ((ComparableStringArray) row).getDelegate()) {
        retVal.add(getUtf8ByteBufferFromString(value));
      }
    } else {
      throw new ISE("Unexpected type %s found", row.getClass().getName());
    }
    return retVal;
  }
}
