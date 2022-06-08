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
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;

/**
 * Reads values written by {@link LongFieldWriter}.
 */
public class LongFieldReader implements FieldReader
{
  LongFieldReader()
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
        ValueType.LONG,
        makeColumnValueSelector(memory, fieldPointer),
        extractionFn
    );
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  private static class Selector implements LongColumnSelector
  {
    private final Memory memory;
    private final ReadableFieldPointer fieldPointer;

    private Selector(final Memory memory, final ReadableFieldPointer fieldPointer)
    {
      this.memory = memory;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public long getLong()
    {
      assert !isNull();
      final long bits = memory.getLong(fieldPointer.position() + Byte.BYTES);
      return Long.reverseBytes(bits) ^ Long.MIN_VALUE;
    }

    @Override
    public boolean isNull()
    {
      return memory.getByte(fieldPointer.position()) == LongFieldWriter.NULL_BYTE;
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }
}
