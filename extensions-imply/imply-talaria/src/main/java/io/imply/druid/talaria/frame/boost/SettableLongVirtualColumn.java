/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.boost;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;

import java.util.Collections;
import java.util.List;

public class SettableLongVirtualColumn implements VirtualColumn
{
  private final String columnName;
  private long theValue = 0;

  public SettableLongVirtualColumn(final String columnName)
  {
    this.columnName = columnName;
  }

  @Override
  public String getOutputName()
  {
    return columnName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
  {
    return new LongColumnSelector()
    {
      @Override
      public long getLong()
      {
        return theValue;
      }

      @Override
      public boolean isNull()
      {
        return false;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Do nothing.
      }
    };
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG).setHasNulls(false);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.emptyList();
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException();
  }

  public long getValue()
  {
    return theValue;
  }

  public void setValue(long theValue)
  {
    this.theValue = theValue;
  }
}
