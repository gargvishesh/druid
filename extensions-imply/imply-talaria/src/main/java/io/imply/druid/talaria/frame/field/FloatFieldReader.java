/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;

public class FloatFieldReader implements FieldReader
{
  FloatFieldReader()
  {
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer)
  {
    return new Selector(memory, fieldPointer);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    return ValueTypes.makeNumericWrappingDimensionSelector(
        ValueType.FLOAT,
        makeColumnValueSelector(memory, fieldPointer),
        extractionFn
    );
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  private static class Selector implements FloatColumnSelector
  {
    private final Memory dataRegion;
    private final ReadableFieldPointer fieldPointer;

    private Selector(final Memory dataRegion, final ReadableFieldPointer fieldPointer)
    {
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public float getFloat()
    {
      assert !isNull();
      final int bits = dataRegion.getInt(fieldPointer.position() + Byte.BYTES);
      return FloatFieldWriter.detransform(bits);
    }

    @Override
    public boolean isNull()
    {
      return dataRegion.getByte(fieldPointer.position()) == FloatFieldWriter.NULL_BYTE;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }
}
