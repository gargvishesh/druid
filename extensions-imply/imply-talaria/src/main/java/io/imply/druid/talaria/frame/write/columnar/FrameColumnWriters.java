/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write.columnar;

import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;

public class FrameColumnWriters
{
  public static final byte TYPE_LONG = 1;
  public static final byte TYPE_FLOAT = 2;
  public static final byte TYPE_DOUBLE = 3;
  public static final byte TYPE_STRING = 4;
  public static final byte TYPE_COMPLEX = 5;

  private FrameColumnWriters()
  {
    // No instantiation.
  }

  /**
   * Helper used by {@link ColumnarFrameWriterFactory}.
   *
   * @throws UnsupportedColumnTypeException if "type" cannot be handled
   */
  static FrameColumnWriter create(
      final ColumnSelectorFactory columnSelectorFactory,
      final MemoryAllocator allocator,
      final String column,
      final ColumnType type
  )
  {
    if (type == null) {
      throw new UnsupportedColumnTypeException(column, null);
    }

    switch (type.getType()) {
      case LONG:
        return makeLongWriter(columnSelectorFactory, allocator, column);
      case FLOAT:
        return makeFloatWriter(columnSelectorFactory, allocator, column);
      case DOUBLE:
        return makeDoubleWriter(columnSelectorFactory, allocator, column);
      case STRING:
        return makeStringWriter(columnSelectorFactory, allocator, column);
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
            return makeStringArrayWriter(columnSelectorFactory, allocator, column);
          default:
            throw new UnsupportedColumnTypeException(column, type);
        }
      case COMPLEX:
        return makeComplexWriter(columnSelectorFactory, allocator, column, type.getComplexTypeName());
      default:
        throw new UnsupportedColumnTypeException(column, type);
    }
  }

  private static LongFrameColumnWriter makeLongWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new LongFrameColumnWriter(selector, allocator, hasNullsForNumericWriter(capabilities));
  }

  private static FloatFrameColumnWriter makeFloatWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new FloatFrameColumnWriter(selector, allocator, hasNullsForNumericWriter(capabilities));
  }

  private static DoubleFrameColumnWriter makeDoubleWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new DoubleFrameColumnWriter(selector, allocator, hasNullsForNumericWriter(capabilities));
  }

  private static StringFrameColumnWriter makeStringWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
    return new StringFrameColumnWriterImpl(
        selector,
        allocator,
        capabilities == null || capabilities.hasMultipleValues().isMaybeTrue()
    );
  }

  private static StringFrameColumnWriter makeStringArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnValueSelector selector = selectorFactory.makeColumnValueSelector(columnName);
    return new StringArrayFrameColumnWriter(
        selector,
        allocator,
        true
    );
  }

  private static ComplexFrameColumnWriter makeComplexWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName,
      @Nullable final String columnTypeName
  )
  {
    if (columnTypeName == null) {
      throw new ISE("No complexTypeName, cannot write column [%s]", columnName);
    }

    final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(columnTypeName);
    if (serde == null) {
      throw new ISE("No serde for complexTypeName[%s], cannot write column [%s]", columnTypeName, columnName);
    }

    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new ComplexFrameColumnWriter(selector, allocator, serde);
  }

  private static boolean hasNullsForNumericWriter(final ColumnCapabilities capabilities)
  {
    if (NullHandling.replaceWithDefault()) {
      return false;
    } else if (capabilities == null) {
      return true;
    } else if (capabilities.getType().isNumeric()) {
      return capabilities.hasNulls().isMaybeTrue();
    } else {
      // Reading
      return true;
    }
  }
}
