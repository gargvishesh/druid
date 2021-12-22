/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.nested.column.NestedDataComplexColumn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.virtual.VirtualColumnCacheHelper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class NestedFieldVirtualColumn implements VirtualColumn
{
  private final String columnName;
  private final String outputName;

  @JsonCreator
  public NestedFieldVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("outputName") String outputName
  )
  {
    this.columnName = columnName;
    this.outputName = outputName;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_USER_DEFINED).appendString("nested-field")
                                                                                   .appendString(columnName)
                                                                                   .build();
  }

  @JsonProperty
  @Override
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {

    final Pair<String, String> columnAndPath = VirtualColumns.splitColumnName(this.columnName);
    final NestedDataComplexColumn complexColumn = getNestedDataComplexColumn(columnSelector, columnAndPath);
    if (complexColumn == null) {
      return DimensionSelector.constant(null);
    }
    return complexColumn.makeDimensionSelector(columnAndPath.rhs, offset, dimensionSpec.getExtractionFn());
  }


  @Nullable
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    final Pair<String, String> columnAndPath = VirtualColumns.splitColumnName(this.columnName);
    final NestedDataComplexColumn complexColumn = getNestedDataComplexColumn(columnSelector, columnAndPath);
    if (complexColumn == null) {
      return NilColumnValueSelector.instance();
    }
    return complexColumn.makeColumnValueSelector(columnAndPath.rhs, offset);
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return true;
  }

  @Nullable
  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final Pair<String, String> columnAndPath = VirtualColumns.splitColumnName(this.columnName);
    final NestedDataComplexColumn complexColumn = getNestedDataComplexColumn(columnSelector, columnAndPath);
    if (complexColumn == null) {
      return NilVectorSelector.create(offset);
    }
    return complexColumn.makeSingleValueDimensionVectorSelector(columnAndPath.rhs, offset);
  }

  @Nullable
  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final Pair<String, String> columnAndPath = VirtualColumns.splitColumnName(this.columnName);
    final NestedDataComplexColumn complexColumn = getNestedDataComplexColumn(columnSelector, columnAndPath);
    if (complexColumn == null) {
      return NilVectorSelector.create(offset);
    }
    return complexColumn.makeVectorObjectSelector(columnAndPath.rhs, offset);
  }

  @Nullable
  @Override
  public BitmapIndex getBitmapIndex(
      String columnName,
      ColumnSelector selector
  )
  {
    final Pair<String, String> columnAndPath = VirtualColumns.splitColumnName(this.columnName);
    final NestedDataComplexColumn complexColumn = getNestedDataComplexColumn(selector, columnAndPath);
    if (complexColumn == null) {
      return null;
    }
    return complexColumn.makeBitmapIndex(columnAndPath.rhs);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                 .setDictionaryEncoded(true)
                                 .setHasMultipleValues(false)
                                 .setDictionaryValuesUnique(true)
                                 .setDictionaryValuesSorted(true)
                                 .setHasBitmapIndexes(true);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(VirtualColumns.splitColumnName(columnName).lhs);
  }

  @Override
  public boolean usesDotNotation()
  {
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NestedFieldVirtualColumn that = (NestedFieldVirtualColumn) o;
    return columnName.equals(that.columnName) && outputName.equals(that.outputName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, outputName);
  }

  @Nullable
  private NestedDataComplexColumn getNestedDataComplexColumn(
      ColumnSelector columnSelector,
      Pair<String, String> columnAndPath
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(columnAndPath.lhs);
    if (holder == null) {
      return null;
    }
    BaseColumn theColumn = holder.getColumn();
    if (!(theColumn instanceof NestedDataComplexColumn)) {
      throw new IAE("Invalid column type [%s]", theColumn.getClass());
    }
    return (NestedDataComplexColumn) theColumn;
  }
}
