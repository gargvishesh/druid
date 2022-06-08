/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read.columnar;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.write.columnar.FloatFrameColumnWriter;
import io.imply.druid.talaria.frame.write.columnar.FrameColumnWriters;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.BaseFloatVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class FloatFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  FloatFrameColumnReader(final int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    validate(frame);

    final Memory memory = frame.region(columnNumber);
    final boolean hasNulls = getHasNulls(memory);

    return new ColumnPlus(
        new FloatFrameColumn(frame, hasNulls, memory),
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.FLOAT)
                              .setHasNulls(NullHandling.sqlCompatible() && hasNulls),
        frame.numRows()
    );
  }

  private void validate(final Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    final long memorySize = memory.getCapacity();

    // Check if column is big enough for a header
    if (memorySize < FloatFrameColumnWriter.DATA_OFFSET) {
      throw new ISE("Column is not big enough for a header");
    }

    final byte typeCode = memory.getByte(0);
    if (typeCode != FrameColumnWriters.TYPE_FLOAT) {
      throw new ISE("Column does not have the correct type code");
    }

    final boolean hasNulls = getHasNulls(memory);
    final int sz = FloatFrameColumnWriter.valueSize(hasNulls);

    // Check column length again, now that we know exactly how long it should be.
    if (memorySize != FloatFrameColumnWriter.DATA_OFFSET + (long) sz * frame.numRows()) {
      throw new ISE("Column does not have the correct length");
    }
  }

  private static boolean getHasNulls(final Memory memory)
  {
    return memory.getByte(Byte.BYTES) != 0;
  }

  private static class FloatFrameColumn implements NumericColumn
  {
    private final Frame frame;
    private final boolean hasNulls;
    private final int sz;
    private final Memory memory;
    private final long memoryPosition;

    private FloatFrameColumn(
        final Frame frame,
        final boolean hasNulls,
        final Memory memory
    )
    {
      this.frame = frame;
      this.hasNulls = hasNulls;
      this.sz = FloatFrameColumnWriter.valueSize(hasNulls);
      this.memory = memory;
      this.memoryPosition = FloatFrameColumnWriter.DATA_OFFSET;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final ReadableOffset offset)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float getFloat()
        {
          assert NullHandling.replaceWithDefault() || !isNull();
          return FloatFrameColumn.this.getFloat(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public boolean isNull()
        {
          return FloatFrameColumn.this.isNull(frame.physicalRow(offset.getOffset()));
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
      class FloatFrameColumnVectorValueSelector extends BaseFloatVectorValueSelector
      {
        private final float[] floatVector;
        private final boolean[] nullVector;

        private int id = ReadableVectorInspector.NULL_ID;

        private FloatFrameColumnVectorValueSelector()
        {
          super(theOffset);
          this.floatVector = new float[offset.getMaxVectorSize()];
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
        public float[] getFloatVector()
        {
          computeVectorsIfNeeded();
          return floatVector;
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
              floatVector[i] = getFloat(physicalRow);

              if (hasNulls) {
                nullVector[i] = isNull(physicalRow);
              }
            }
          } else {
            final int[] offsets = offset.getOffsets();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(offsets[i]);
              floatVector[i] = getFloat(physicalRow);

              if (hasNulls) {
                nullVector[i] = isNull(physicalRow);
              }
            }
          }

          id = offset.getId();
        }
      }

      return new FloatFrameColumnVectorValueSelector();
    }

    @Override
    public int length()
    {
      return frame.numRows();
    }

    @Override
    public long getLongSingleValueRow(final int rowNum)
    {
      // Need bounds checking, since getFloat(physicalRow) doesn't do it.
      if (rowNum < 0 || rowNum >= frame.numRows()) {
        throw new ISE("Row [%d] out of bounds", rowNum);
      }

      return (long) getFloat(frame.physicalRow(rowNum));
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
        return memory.getByte(rowPosition) != 0;
      } else {
        return false;
      }
    }

    private float getFloat(final int physicalRow)
    {
      final long rowPosition = memoryPosition + (long) sz * physicalRow;

      if (hasNulls) {
        return memory.getFloat(rowPosition + 1);
      } else {
        return memory.getFloat(rowPosition);
      }
    }
  }
}
