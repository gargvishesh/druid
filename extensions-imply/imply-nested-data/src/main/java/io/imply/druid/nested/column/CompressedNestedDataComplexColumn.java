/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * {@link UncompressedNestedDataComplexColumn} but with {@link CompressedVariableSizedBlobColumn} instead of a
 * {@link GenericIndexed} for the raw values
 */
public final class CompressedNestedDataComplexColumn extends UncompressedNestedDataComplexColumn
{
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private CompressedVariableSizedBlobColumn compressedRawColumn;

  private static final ObjectStrategy<Object> STRATEGY = NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy();

  public CompressedNestedDataComplexColumn(
      NestedDataColumnMetadata metadata,
      ColumnConfig columnConfig,
      CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      GenericIndexed<String> fields,
      NestedLiteralTypeInfo fieldInfo,
      GenericIndexed<String> stringDictionary,
      FixedIndexed<Long> longDictionary,
      FixedIndexed<Double> doubleDictionary,
      SmooshedFileMapper fileMapper
  )
  {
    super(
        metadata,
        columnConfig,
        null,
        nullValues,
        fields,
        fieldInfo,
        stringDictionary,
        longDictionary,
        doubleDictionary,
        fileMapper
    );
    this.compressedRawColumnSupplier = compressedRawColumnSupplier;
  }

  @Nullable
  @Override
  public Object getRowValue(int rowNum)
  {
    if (nullValues.get(rowNum)) {
      return null;
    }

    if (compressedRawColumn == null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }

    final ByteBuffer valueBuffer = compressedRawColumn.get(rowNum);
    return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    if (compressedRawColumn == null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        if (nullValues.get(offset.getOffset())) {
          return null;
        }
        final ByteBuffer valueBuffer = compressedRawColumn.get(offset.getOffset());
        return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      }

      @Override
      public Class classOfObject()
      {
        return getClazz();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", CompressedNestedDataComplexColumn.this);
      }
    };
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    if (compressedRawColumn == null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }
    return new VectorObjectSelector()
    {
      final Object[] vector = new Object[offset.getMaxVectorSize()];

      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          final int startOffset = offset.getStartOffset();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getForOffset(startOffset + i);
          }
        } else {
          final int[] offsets = offset.getOffsets();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getForOffset(offsets[i]);

          }
        }

        id = offset.getId();
        return vector;
      }

      @Nullable
      private Object getForOffset(int offset)
      {
        if (nullValues.get(offset)) {
          // todo (clint): maybe can use bitmap batch operations for nulls?
          return null;
        }
        final ByteBuffer valueBuffer = compressedRawColumn.get(offset);
        return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }
    };
  }
}
