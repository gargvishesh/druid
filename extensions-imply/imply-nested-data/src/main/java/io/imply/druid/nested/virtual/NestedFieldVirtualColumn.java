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
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
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
  private final List<PathFinder.PathPart> parts;
  /**
   * type information for nested fields is completely absent in the SQL planner, so it guesses the best it can
   * from the context of how something is being used. This might not be the same as if it had actual type information,
   * but, we try to stick with whatever we chose there to do the best we can for now
   */
  @Nullable
  private final ColumnType expectedType;

  private final boolean processFromRaw;

  @JsonCreator
  public NestedFieldVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("path") String path,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("expectedType") @Nullable ColumnType expectedType,
      @JsonProperty("processFromRaw") @Nullable Boolean processFromRaw
  )
  {
    this.columnName = columnName;
    this.outputName = outputName;
    this.parts = PathFinder.parseJqPath(path);
    this.path = PathFinder.toNormalizedJqPath(parts);
    this.expectedType = expectedType;
    this.processFromRaw = processFromRaw == null ? false : processFromRaw;
  }

  @VisibleForTesting
  public NestedFieldVirtualColumn(
      String columnName,
      String path,
      String outputName
  )
  {
    this(columnName, path, outputName, null, null);
  }

  @VisibleForTesting
  public NestedFieldVirtualColumn(
      String columnName,
      String path,
      String outputName,
      ColumnType expectedType
  )
  {
    this(columnName, path, outputName, expectedType, null);
  }

  public NestedFieldVirtualColumn(
      String columnName,
      String outputName,
      ColumnType expectedType,
      List<PathFinder.PathPart> parts,
      String normalizedPath
  )
  {
    this.columnName = columnName;
    this.outputName = outputName;
    this.parts = parts;
    this.path = normalizedPath;
    this.expectedType = expectedType;
    this.processFromRaw = false;
  }

  public NestedFieldVirtualColumn(
      String columnName,
      String outputName,
      ColumnType expectedType,
      List<PathFinder.PathPart> parts,
      String normalizedPath,
      boolean processFromRaw
  )
  {
    this.columnName = columnName;
    this.outputName = outputName;
    this.parts = parts;
    this.path = normalizedPath;
    this.expectedType = expectedType;
    this.processFromRaw = processFromRaw;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_USER_DEFINED).appendString("nested-field")
                                                                                   .appendString(columnName)
                                                                                   .appendString(path)
                                                                                   .appendBoolean(processFromRaw)
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

  @JsonProperty
  public ColumnType getExpectedType()
  {
    return expectedType;
  }

  @JsonProperty
  public boolean isProcessFromRaw()
  {
    return processFromRaw;
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
        if (val == null || val instanceof String) {
          return (String) val;
        }
        return String.valueOf(val);
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

    return processFromRaw
           ? new RawFieldColumnSelector(baseSelector, parts)
           : new RawFieldLiteralColumnValueSelector(baseSelector, parts);
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
    return processFromRaw
           ? new RawFieldColumnSelector(column.makeColumnValueSelector(offset), parts)
           : column.makeColumnValueSelector(path, offset);
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
    return processFromRaw
           ? new RawFieldVectorObjectSelector(column.makeVectorObjectSelector(offset), parts)
           : column.makeVectorObjectSelector(path, offset);
  }

  @Nullable
  @Override
  public VectorValueSelector makeVectorValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(columnSelector, this.columnName);
    if (column == null) {
      return NilVectorSelector.create(offset);
    }
    return column.makeVectorValueSelector(path, offset);
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnSelector selector
  )
  {
    final NestedDataComplexColumn column = NestedDataComplexColumn.fromColumnSelector(selector, this.columnName);

    if (column == null) {
      return null;
    }
    return column.getColumnIndexSupplier(path);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(expectedType != null ? expectedType : ColumnType.STRING);
  }

  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    final ColumnCapabilities complexCapabilites = inspector.getColumnCapabilities(this.columnName);
    if (complexCapabilites != null && complexCapabilites.isDictionaryEncoded().isTrue()) {
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(expectedType != null ? expectedType : ColumnType.STRING)
                                   .setDictionaryEncoded(true)
                                   .setDictionaryValuesSorted(true)
                                   .setDictionaryValuesUnique(true)
                                   .setHasBitmapIndexes(true);
    }
    return capabilities(columnName);
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
    return columnName.equals(that.columnName) && outputName.equals(that.outputName) && path.equals(that.path)
           && Objects.equals(expectedType, that.expectedType) && processFromRaw == that.processFromRaw;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, path, outputName, expectedType, processFromRaw);
  }

  @Override
  public String toString()
  {
    return "NestedFieldVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", path='" + path + '\'' +
           ", outputName='" + outputName + '\'' +
           ", typeHint='" + expectedType + '\'' +
           ", allowFallback=" + processFromRaw +
           '}';
  }


  /**
   * Process the "raw" data to extract literals with {@link PathFinder#findLiteral(Object, List)}. Like
   * {@link RawFieldColumnSelector} but only literals and does not wrap the results in {@link StructuredData}.
   *
   * This is used as a selector on realtime data when the native field columns are not available.
   */
  public static class RawFieldLiteralColumnValueSelector implements ColumnValueSelector<Object>
  {
    private final ColumnValueSelector baseSelector;
    private final List<PathFinder.PathPart> parts;

    public RawFieldLiteralColumnValueSelector(ColumnValueSelector baseSelector, List<PathFinder.PathPart> parts)
    {
      this.baseSelector = baseSelector;
      this.parts = parts;
    }

    @Override
    public double getDouble()
    {
      Object o = getObject();
      if (o instanceof Number) {
        return ((Number) o).doubleValue();
      }
      return 0.0;
    }

    @Override
    public float getFloat()
    {
      Object o = getObject();
      if (o instanceof Number) {
        return ((Number) o).floatValue();
      }
      return 0f;
    }

    @Override
    public long getLong()
    {
      Object o = getObject();
      if (o instanceof Number) {
        return ((Number) o).longValue();
      }
      return 0L;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseSelector", baseSelector);
    }

    @Override
    public boolean isNull()
    {
      return !(getObject() instanceof Number);
    }

    @Nullable
    @Override
    public Object getObject()
    {
      StructuredData data = StructuredData.possiblyWrap(baseSelector.getObject());
      return PathFinder.findLiteral(data == null ? null : data.getValue(), parts);
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }
  }

  /**
   * Process the "raw" data to extract values with {@link PathFinder#find(Object, List)}, wrapping the result in
   * {@link StructuredData}
   */
  public static class RawFieldColumnSelector implements ColumnValueSelector<Object>
  {
    private final ColumnValueSelector baseSelector;
    private final List<PathFinder.PathPart> parts;

    public RawFieldColumnSelector(ColumnValueSelector baseSelector, List<PathFinder.PathPart> parts)
    {
      this.baseSelector = baseSelector;
      this.parts = parts;
    }

    @Override
    public double getDouble()
    {
      Object o = getObject();
      if (o instanceof Number) {
        return ((Number) o).doubleValue();
      }
      return 0.0;
    }

    @Override
    public float getFloat()
    {
      Object o = getObject();
      if (o instanceof Number) {
        return ((Number) o).floatValue();
      }
      return 0f;
    }

    @Override
    public long getLong()
    {
      Object o = getObject();
      if (o instanceof Number) {
        return ((Number) o).longValue();
      }
      return 0L;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseSelector", baseSelector);
    }

    @Override
    public boolean isNull()
    {
      return !(getObject() instanceof Number);
    }

    @Nullable
    @Override
    public Object getObject()
    {
      StructuredData data = StructuredData.possiblyWrap(baseSelector.getObject());
      return StructuredData.possiblyWrap(PathFinder.find(data == null ? null : data.getValue(), parts));
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }
  }

  /**
   * Process the "raw" data to extract vectors of values with {@link PathFinder#find(Object, List)}, wrapping the result
   * in {@link StructuredData}
   */
  public static class RawFieldVectorObjectSelector implements VectorObjectSelector
  {
    private final VectorObjectSelector baseSelector;
    private final List<PathFinder.PathPart> parts;
    private final Object[] vector;

    public RawFieldVectorObjectSelector(
        VectorObjectSelector baseSelector,
        List<PathFinder.PathPart> parts
    )
    {
      this.baseSelector = baseSelector;
      this.parts = parts;
      this.vector = new Object[baseSelector.getMaxVectorSize()];
    }

    @Override
    public Object[] getObjectVector()
    {
      Object[] baseVector = baseSelector.getObjectVector();
      for (int i = 0; i < baseSelector.getCurrentVectorSize(); i++) {
        vector[i] = compute(baseVector[i]);
      }
      return vector;
    }

    @Override
    public int getMaxVectorSize()
    {
      return baseSelector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return baseSelector.getCurrentVectorSize();
    }

    private Object compute(Object input)
    {
      StructuredData data = StructuredData.possiblyWrap(input);
      return StructuredData.possiblyWrap(PathFinder.find(data == null ? null : data.getValue(), parts));
    }
  }
}
