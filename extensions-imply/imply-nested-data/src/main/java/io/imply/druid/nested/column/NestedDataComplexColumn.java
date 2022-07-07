/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;

public abstract class NestedDataComplexColumn implements ComplexColumn
{
  @Nullable
  public static NestedDataComplexColumn fromColumnSelector(
      ColumnSelector columnSelector,
      String columnName
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(columnName);
    if (holder == null) {
      return null;
    }
    BaseColumn theColumn = holder.getColumn();
    if (theColumn instanceof UncompressedNestedDataComplexColumn) {
      return (UncompressedNestedDataComplexColumn) theColumn;
    }
    throw new IAE(
        "Column [%s] is invalid type, found [%s] instead of [%s]",
        columnName,
        theColumn.getClass(),
        NestedDataComplexColumn.class.getSimpleName()
    );
  }

  private final ConcurrentHashMap<String, ColumnHolder> columns = new ConcurrentHashMap<>();

  public abstract DimensionSelector makeDimensionSelector(String field, ReadableOffset readableOffset, ExtractionFn fn);
  public abstract ColumnValueSelector<?> makeColumnValueSelector(String field, ReadableOffset readableOffset);

  public abstract SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      String field,
      ReadableVectorOffset readableOffset
  );
  public abstract VectorObjectSelector makeVectorObjectSelector(String field, ReadableVectorOffset readableOffset);

  public abstract VectorValueSelector makeVectorValueSelector(String field, ReadableVectorOffset readableOffset);
  public abstract ColumnHolder readNestedFieldColumn(String field);

  @Nullable
  public abstract ColumnIndexSupplier getColumnIndexSupplier(String field);
  @Override
  public Class<?> getClazz()
  {
    return StructuredData.class;
  }

  @Override
  public String getTypeName()
  {
    return NestedDataComplexTypeSerde.TYPE_NAME;
  }

  protected ColumnHolder getColumnHolder(String field)
  {
    return columns.computeIfAbsent(field, this::readNestedFieldColumn);
  }

  public boolean isNumeric(String field)
  {
    return columns.computeIfAbsent(field, this::readNestedFieldColumn).getCapabilities().isNumeric();
  }
}
