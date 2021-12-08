/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.MemoryWithRange;
import io.imply.druid.talaria.frame.write.StringFrameColumnWriter;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  StringFrameColumnReader(int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    validate(frame);

    final MemoryWithRange<Memory> memoryRange = frame.column(columnNumber);
    final boolean multiValue = isMultiValue(memoryRange);
    final long startOfStringLengthSection = getStartOfStringLengthSection(frame.numRows(), multiValue);
    final long startOfStringDataSection = getStartOfStringDataSection(memoryRange, frame.numRows(), multiValue);

    return new ColumnPlus(
        new StringFrameColumn(frame, multiValue, memoryRange, startOfStringLengthSection, startOfStringDataSection),
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                    .setHasMultipleValues(multiValue)
                                    .setDictionaryEncoded(false)
                                    .setHasBitmapIndexes(false)
                                    .setHasSpatialIndexes(false)
                                    .setHasNulls(ColumnCapabilities.Capable.UNKNOWN),
        frame.numRows()
    );
  }

  private void validate(@SuppressWarnings("unused") final Frame frame)
  {
    // TODO(gianm): Validate
  }

  private static boolean isMultiValue(final MemoryWithRange<Memory> memoryRange)
  {
    return memoryRange.memory().getByte(memoryRange.start() + 1) == 1;
  }

  private static int getCumulativeRowLength(
      final MemoryWithRange<Memory> memoryRange,
      final int physicalRow
  )
  {
    // Note: only valid to call this if multiValue = true.
    return memoryRange.memory()
                      .getInt(memoryRange.start()
                              + StringFrameColumnWriter.DATA_OFFSET
                              + (long) Integer.BYTES * physicalRow);
  }

  private static long getStartOfStringLengthSection(
      final int numRows,
      final boolean multiValue
  )
  {
    if (multiValue) {
      return StringFrameColumnWriter.DATA_OFFSET + (long) Integer.BYTES * numRows;
    } else {
      return StringFrameColumnWriter.DATA_OFFSET;
    }
  }

  private static long getStartOfStringDataSection(
      final MemoryWithRange<Memory> memoryRange,
      final int numRows,
      final boolean multiValue
  )
  {
    final int totalNumValues;

    if (multiValue) {
      totalNumValues = getCumulativeRowLength(memoryRange, numRows - 1);
    } else {
      totalNumValues = numRows;
    }

    return getStartOfStringLengthSection(numRows, multiValue) + (long) Integer.BYTES * totalNumValues;
  }

  @VisibleForTesting
  static class StringFrameColumn implements DictionaryEncodedColumn<String>
  {
    private final Frame frame;
    private final boolean multiValue;
    private final MemoryWithRange<Memory> memoryRange;
    private final long startOfStringLengthSection;
    private final long startOfStringDataSection;

    private StringFrameColumn(
        Frame frame,
        boolean multiValue,
        MemoryWithRange<Memory> memoryRange,
        long startOfStringLengthSection,
        long startOfStringDataSection
    )
    {
      this.frame = frame;
      this.multiValue = multiValue;
      this.memoryRange = memoryRange;
      this.startOfStringLengthSection = startOfStringLengthSection;
      this.startOfStringDataSection = startOfStringDataSection;
    }

    @Override
    public boolean hasMultipleValues()
    {
      // Only used in segment tests that don't run on frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public int getSingleValueRow(int rowNum)
    {
      // Only used in segment tests that don't run on frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexedInts getMultiValueRow(int rowNum)
    {
      // Only used in segment tests that don't run on frames.
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      // Only used on columns from segments, not frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public int lookupId(String name)
    {
      // Only used on columns from segments, not frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public int getCardinality()
    {
      // Unknown, so return Integer.MAX_VALUE.
      // TODO(gianm): Is this OK? It's what the interface javadocs say to do, but we should double-check usage.
      //   May make more sense to switch the interface to ask for CARDINALITY_UNKNOWN
      return Integer.MAX_VALUE;
    }

    @Override
    public DimensionSelector makeDimensionSelector(ReadableOffset offset, @Nullable ExtractionFn extractionFn)
    {
      if (multiValue) {
        class MultiValueSelector implements DimensionSelector
        {
          private int currentRow = -1;
          private List<ByteBuffer> currentValues = null;
          private final RangeIndexedInts indexedInts = new RangeIndexedInts();

          @Override
          public int getValueCardinality()
          {
            return CARDINALITY_UNKNOWN;
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            populate();
            final ByteBuffer buf = currentValues.get(id);
            final String s = buf == null ? null : StringUtils.fromUtf8(buf.duplicate());
            return extractionFn == null ? s : extractionFn.apply(s);
          }

          @Nullable
          @Override
          public ByteBuffer lookupNameUtf8(int id)
          {
            assert supportsLookupNameUtf8();
            populate();
            return currentValues.get(id);
          }

          @Override
          public boolean supportsLookupNameUtf8()
          {
            return extractionFn == null;
          }

          @Override
          public boolean nameLookupPossibleInAdvance()
          {
            return false;
          }

          @Nullable
          @Override
          public IdLookup idLookup()
          {
            return null;
          }

          @Override
          public IndexedInts getRow()
          {
            populate();
            return indexedInts;
          }

          @Override
          public ValueMatcher makeValueMatcher(@Nullable String value)
          {
            return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
          }

          @Override
          public ValueMatcher makeValueMatcher(Predicate<String> predicate)
          {
            return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
          }

          @Nullable
          @Override
          public Object getObject()
          {
            return defaultGetObject();
          }

          @Override
          public Class<?> classOfObject()
          {
            return String.class;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // Do nothing.
          }

          private void populate()
          {
            final int row = offset.getOffset();

            if (row != currentRow) {
              currentValues = getRowAsListUtf8(frame.physicalRow(row));
              indexedInts.setSize(currentValues.size());
              currentRow = row;
            }
          }
        }

        return new MultiValueSelector();
      } else {
        class SingleValueSelector extends BaseSingleValueDimensionSelector
        {
          @Nullable
          @Override
          protected String getValue()
          {
            final String s = getString(frame.physicalRow(offset.getOffset()));
            return extractionFn == null ? s : extractionFn.apply(s);
          }

          @Nullable
          @Override
          public ByteBuffer lookupNameUtf8(int id)
          {
            assert supportsLookupNameUtf8();
            return getStringUtf8(frame.physicalRow(offset.getOffset()));
          }

          @Override
          public boolean supportsLookupNameUtf8()
          {
            return extractionFn == null;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // Do nothing.
          }
        }

        return new SingleValueSelector();
      }
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset offset)
    {
      // Callers should use object selectors, because we have no dictionary.
      throw new UnsupportedOperationException();
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
    {
      // Callers should use object selectors, because we have no dictionary.
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(final ReadableVectorOffset offset)
    {
      class StringFrameVectorObjectSelector implements VectorObjectSelector
      {
        private final Object[] vector = new Object[offset.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public Object[] getObjectVector()
        {
          computeVectorIfNeeded();
          return vector;
        }

        @Override
        public int getMaxVectorSize()
        {
          return offset.getMaxVectorSize();
        }

        @Override
        public int getCurrentVectorSize()
        {
          return offset.getCurrentVectorSize();
        }

        private void computeVectorIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }

          if (offset.isContiguous()) {
            final int start = offset.getStartOffset();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(i + start);
              vector[i] = getRowAsObject(physicalRow, true);
            }
          } else {
            final int[] offsets = offset.getOffsets();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(offsets[i]);
              vector[i] = getRowAsObject(physicalRow, true);
            }
          }

          id = offset.getId();
        }
      }

      return new StringFrameVectorObjectSelector();
    }

    @Override
    public int length()
    {
      return frame.numRows();
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    @Nullable
    private ByteBuffer getStringUtf8(final int index)
    {
      final Memory memory = memoryRange.memory();

      final long dataStart;
      final long dataEnd =
          startOfStringDataSection +
          memory.getInt(memoryRange.start() + startOfStringLengthSection + (long) Integer.BYTES * index);

      if (index == 0) {
        dataStart = startOfStringDataSection;
      } else {
        dataStart =
            startOfStringDataSection +
            memory.getInt(memoryRange.start() + startOfStringLengthSection + (long) Integer.BYTES * (index - 1));
      }

      final int dataLength = Ints.checkedCast(dataEnd - dataStart);

      if ((dataLength == 0 && NullHandling.replaceWithDefault()) ||
          (dataLength == 1 && memory.getByte(memoryRange.start() + dataStart) == StringFrameColumnWriter.NULL_MARKER)) {
        return null;
      }

      if (memory.hasByteBuffer()) {
        // Avoid copy
        final ByteBuffer byteBuffer = memory.getByteBuffer().duplicate();
        byteBuffer.limit(Ints.checkedCast(memory.getRegionOffset(memoryRange.start() + dataEnd)));
        byteBuffer.position(Ints.checkedCast(memory.getRegionOffset(memoryRange.start() + dataStart)));
        return byteBuffer;
      } else {
        final byte[] stringData = new byte[dataLength];
        memory.getByteArray(memoryRange.start() + dataStart, stringData, 0, stringData.length);
        return ByteBuffer.wrap(stringData);
      }
    }

    @Nullable
    private String getString(final int index)
    {
      final ByteBuffer stringUtf8 = getStringUtf8(index);

      if (stringUtf8 == null) {
        return null;
      } else {
        return StringUtils.fromUtf8(stringUtf8.duplicate());
      }
    }

    @Nullable
    private Object getRowAsObject(final int physicalRow, final boolean decode)
    {
      if (multiValue) {
        final int cumulativeRowLength = getCumulativeRowLength(memoryRange, physicalRow);
        final int rowLength = physicalRow == 0
                              ? cumulativeRowLength
                              : cumulativeRowLength - getCumulativeRowLength(memoryRange, physicalRow - 1);

        if (rowLength == 0) {
          return Collections.emptyList();
        } else if (rowLength == 1) {
          final int index = cumulativeRowLength - 1;
          return decode ? getString(index) : getStringUtf8(index);
        } else {
          final List<Object> row = new ArrayList<>(rowLength);

          for (int i = 0; i < rowLength; i++) {
            final int index = cumulativeRowLength - rowLength + i;
            row.add(decode ? getString(index) : getStringUtf8(index));
          }

          return row;
        }
      } else {
        return decode ? getString(physicalRow) : getStringUtf8(physicalRow);
      }
    }

    private List<ByteBuffer> getRowAsListUtf8(final int physicalRow)
    {
      final Object object = getRowAsObject(physicalRow, false);

      if (object instanceof List) {
        //noinspection unchecked
        return (List<ByteBuffer>) object;
      } else {
        return Collections.singletonList((ByteBuffer) object);
      }
    }
  }
}
