/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read.columnar;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.write.columnar.ComplexFrameColumnWriter;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;

public class ComplexFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  ComplexFrameColumnReader(final int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    // TODO(gianm): implement validation

    final Memory memory = frame.region(columnNumber);
    final int typeNameLength = memory.getInt(Byte.BYTES);
    final byte[] typeNameBytes = new byte[typeNameLength];

    memory.getByteArray(Byte.BYTES + Integer.BYTES, typeNameBytes, 0, typeNameLength);

    final String typeName = StringUtils.fromUtf8(typeNameBytes);
    final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

    if (serde == null) {
      throw new ISE("Cannot read column with complexTypeName[%s]", typeName);
    }

    final long startOfOffsetSection = Byte.BYTES + Integer.BYTES + typeNameLength;
    final long startOfDataSection = startOfOffsetSection + (long) frame.numRows() * Integer.BYTES;

    return new ColumnPlus(
        new ComplexFrameColumn(
            frame,
            serde,
            memory,
            startOfOffsetSection,
            startOfDataSection
        ),
        new ColumnCapabilitiesImpl().setType(ColumnType.ofComplex(typeName)),
        frame.numRows()
    );
  }

  private static class ComplexFrameColumn implements ComplexColumn
  {
    private final Frame frame;
    private final ComplexMetricSerde serde;
    private final Memory memory;
    private final long startOfOffsetSection;
    private final long startOfDataSection;

    private ComplexFrameColumn(
        final Frame frame,
        final ComplexMetricSerde serde,
        final Memory memory,
        final long startOfOffsetSection,
        final long startOfDataSection
    )
    {
      this.frame = frame;
      this.serde = serde;
      this.memory = memory;
      this.startOfOffsetSection = startOfOffsetSection;
      this.startOfDataSection = startOfDataSection;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final ReadableOffset offset)
    {
      return new ObjectColumnSelector<Object>()
      {
        @Nullable
        @Override
        public Object getObject()
        {
          return ComplexFrameColumn.this.getObjectForPhysicalRow(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public Class<?> classOfObject()
        {
          return serde.getExtractor().extractedClass();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Do nothing.
        }
      };
    }

    @Override
    public Class<?> getClazz()
    {
      return serde.getClass();
    }

    @Override
    public String getTypeName()
    {
      return serde.getTypeName();
    }

    @Override
    public Object getRowValue(int rowNum)
    {
      // Need bounds checking, since getObjectForPhysicalRow doesn't do it.
      if (rowNum < 0 || rowNum >= frame.numRows()) {
        throw new ISE("Row [%d] out of bounds", rowNum);
      }

      return getObjectForPhysicalRow(frame.physicalRow(rowNum));
    }

    @Override
    public int getLength()
    {
      return frame.numRows();
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    @Nullable
    private Object getObjectForPhysicalRow(final int physicalRow)
    {
      final long endOffset =
          startOfDataSection + memory.getInt(startOfOffsetSection + (long) Integer.BYTES * physicalRow);
      final long startOffset;

      if (physicalRow == 0) {
        startOffset = startOfDataSection;
      } else {
        startOffset =
            startOfDataSection + memory.getInt(startOfOffsetSection + (long) Integer.BYTES * (physicalRow - 1));
      }

      if (memory.getByte(startOffset) == ComplexFrameColumnWriter.NULL_MARKER) {
        return null;
      } else {
        final int payloadLength = Ints.checkedCast(endOffset - startOffset - Byte.BYTES);
        final byte[] complexBytes = new byte[payloadLength];
        memory.getByteArray(startOffset + Byte.BYTES, complexBytes, 0, payloadLength);
        return serde.fromBytes(complexBytes, 0, complexBytes.length);
      }
    }
  }
}
