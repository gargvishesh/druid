/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import io.imply.druid.talaria.frame.MemoryWithRange;
import io.imply.druid.talaria.frame.write.DoubleFrameColumnWriter;
import io.imply.druid.talaria.frame.write.FrameColumnWriters;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class DoubleFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  DoubleFrameColumnReader(final int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    validate(frame);

    final MemoryWithRange<Memory> memoryRange = frame.column(columnNumber);
    final boolean hasNulls = getHasNulls(memoryRange);

    return new ColumnPlus(
        new DoubleFrameColumn(frame, hasNulls, memoryRange),
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE)
                              .setHasNulls(NullHandling.sqlCompatible() && hasNulls),
        frame.numRows()
    );
  }

  private void validate(final Frame frame)
  {
    final MemoryWithRange<Memory> memoryRange = frame.column(columnNumber);
    final long memorySize = memoryRange.end() - memoryRange.start();

    // Check if column is big enough for a header
    if (memorySize < DoubleFrameColumnWriter.DATA_OFFSET) {
      throw new ISE("Column is not big enough for a header");
    }

    final byte typeCode = memoryRange.memory().getByte(memoryRange.start());
    if (typeCode != FrameColumnWriters.TYPE_DOUBLE) {
      throw new ISE("Column does not have the correct type code");
    }

    final boolean hasNulls = getHasNulls(memoryRange);
    final int sz = DoubleFrameColumnWriter.valueSize(hasNulls);

    // Check column length again, now that we know exactly how long it should be.
    if (memorySize != DoubleFrameColumnWriter.DATA_OFFSET + (long) sz * frame.numRows()) {
      throw new ISE("Column does not have the correct length");
    }
  }

  private static boolean getHasNulls(final MemoryWithRange<Memory> memoryRange)
  {
    return memoryRange.memory().getByte(memoryRange.start() + Byte.BYTES) != 0;
  }

  private static class DoubleFrameColumn implements NumericColumn
  {
    private final Frame frame;
    private final boolean hasNulls;
    private final int sz;
    private final MemoryWithRange<Memory> memoryRange;
    private final long memoryPosition;

    private DoubleFrameColumn(
        final Frame frame,
        final boolean hasNulls,
        final MemoryWithRange<Memory> memoryRange
    )
    {
      this.frame = frame;
      this.hasNulls = hasNulls;
      this.sz = DoubleFrameColumnWriter.valueSize(hasNulls);
      this.memoryRange = memoryRange;
      this.memoryPosition = memoryRange.start() + DoubleFrameColumnWriter.DATA_OFFSET;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final ReadableOffset offset)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double getDouble()
        {
          assert NullHandling.replaceWithDefault() || !isNull();
          return DoubleFrameColumn.this.getDouble(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public boolean isNull()
        {
          return DoubleFrameColumn.this.isNull(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Do nothing.
        }
      };
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(final ReadableVectorOffset theOffset)
    {
      class DoubleFrameColumnVectorValueSelector extends BaseDoubleVectorValueSelector
      {
        private final double[] doubleVector;
        private final boolean[] nullVector;

        private int id = ReadableVectorInspector.NULL_ID;

        private DoubleFrameColumnVectorValueSelector()
        {
          super(theOffset);
          this.doubleVector = new double[offset.getMaxVectorSize()];
          this.nullVector = hasNulls ? new boolean[offset.getMaxVectorSize()] : null;
        }

        @Nullable
        @Override
        public boolean[] getNullVector()
        {
          computeVectorsIfNeeded();
          return nullVector;
        }

        @Override
        public double[] getDoubleVector()
        {
          computeVectorsIfNeeded();
          return doubleVector;
        }

        private void computeVectorsIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }

          if (offset.isContiguous()) {
            final int start = offset.getStartOffset();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(i + start);
              doubleVector[i] = getDouble(physicalRow);

              if (hasNulls) {
                nullVector[i] = isNull(physicalRow);
              }
            }
          } else {
            final int[] offsets = offset.getOffsets();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(offsets[i]);
              doubleVector[i] = getDouble(physicalRow);

              if (hasNulls) {
                nullVector[i] = isNull(physicalRow);
              }
            }
          }

          id = offset.getId();
        }
      }

      return new DoubleFrameColumnVectorValueSelector();
    }

    @Override
    public int length()
    {
      return frame.numRows();
    }

    @Override
    public long getLongSingleValueRow(final int rowNum)
    {
      // Need bounds checking, since getDouble(physicalRow) doesn't do it.
      if (rowNum < 0 || rowNum >= frame.numRows()) {
        throw new ISE("Row [%d] out of bounds", rowNum);
      }

      return (long) getDouble(frame.physicalRow(rowNum));
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }

    private boolean isNull(final int physicalRow)
    {
      if (hasNulls) {
        final long rowPosition = memoryPosition + (long) sz * physicalRow;
        return memoryRange.memory().getByte(rowPosition) != 0;
      } else {
        return false;
      }
    }

    private double getDouble(final int physicalRow)
    {
      final long rowPosition = memoryPosition + (long) sz * physicalRow;

      if (hasNulls) {
        return memoryRange.memory().getDouble(rowPosition + 1);
      } else {
        return memoryRange.memory().getDouble(rowPosition);
      }
    }
  }
}
