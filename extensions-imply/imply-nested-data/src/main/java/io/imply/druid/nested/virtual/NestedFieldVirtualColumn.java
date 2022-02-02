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
import io.imply.druid.nested.column.PathFinder;
import io.imply.druid.nested.column.StructuredData;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
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
  private final String path;
  private final String outputName;
  private final List<PathFinder.PathPartFinder> parts;

  @JsonCreator
  public NestedFieldVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("path") String path,
      @JsonProperty("outputName") String outputName
  )
  {
    this.columnName = columnName;
    this.outputName = outputName;
    this.parts = PathFinder.parseJqPath(path);
    this.path = PathFinder.toNormalizedJqPath(parts);
  }

  public NestedFieldVirtualColumn(
      String columnName,
      String outputName,
      List<PathFinder.PathPartFinder> parts,
      String normalizedPath
  )
  {
    this.columnName = columnName;
    this.outputName = outputName;
    this.parts = parts;
    this.path = normalizedPath;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_USER_DEFINED).appendString("nested-field")
                                                                                   .appendString(columnName)
                                                                                   .appendString(path)
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

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    ColumnValueSelector valueSelector = makeColumnValueSelector(dimensionSpec.getOutputName(), factory);

    class FieldDimensionSelector extends BaseSingleValueDimensionSelector
    {
      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("valueSelector", valueSelector);
      }

      @Nullable
      @Override
      protected String getValue()
      {
        Object val = valueSelector.getObject();
        if (val instanceof String) {
          return (String) val;
        }
        return null;
      }
    }
    return new FieldDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    final ColumnValueSelector baseSelector = factory.makeColumnValueSelector(this.columnName);

    class FieldColumnSelector implements ColumnValueSelector<Object>
    {
      @Override
      public double getDouble()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public float getFloat()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public long getLong()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("baseSelector", baseSelector);
      }

      @Override
      public boolean isNull()
      {
        return baseSelector.isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        StructuredData data = (StructuredData) baseSelector.getObject();
        return PathFinder.findStringLiteral(
            data == null ? null : data.getValue(),
            parts
        );
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }
    }
    return new FieldColumnSelector();
  }

  @Nullable
  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(columnSelector, columnName);
    if (column == null) {
      return DimensionSelector.constant(null);
    }
    return column.makeDimensionSelector(path, offset, dimensionSpec.getExtractionFn());
  }


  @Nullable
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(columnSelector, this.columnName);
    if (column == null) {
      return NilColumnValueSelector.instance();
    }
    return column.makeColumnValueSelector(path, offset);
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
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(columnSelector, columnName);
    if (column == null) {
      return NilVectorSelector.create(offset);
    }
    return column.makeSingleValueDimensionVectorSelector(path, offset);
  }

  @Nullable
  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(columnSelector, this.columnName);
    if (column == null) {
      return NilVectorSelector.create(offset);
    }
    return column.makeVectorObjectSelector(path, offset);
  }

  @Nullable
  @Override
  public BitmapIndex getBitmapIndex(
      String columnName,
      ColumnSelector selector
  )
  {
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(selector, this.columnName);

    if (column == null) {
      return null;
    }
    return column.makeBitmapIndex(path);
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
    return Collections.singletonList(columnName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
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
    return columnName.equals(that.columnName) && outputName.equals(that.outputName) && path.equals(that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, path, outputName);
  }

  @Override
  public String toString()
  {
    return "NestedFieldVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", path='" + path + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
