/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ValueTypes;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;

import javax.annotation.Nullable;

/**
 * Returned by {@link FrameColumnReader#readColumn}.
 */
public class ColumnPlus implements ColumnHolder
{
  private final BaseColumn column;
  private final ColumnCapabilities capabilities;
  private final int length;

  ColumnPlus(final BaseColumn column, final ColumnCapabilities capabilities, final int length)
  {
    this.column = column;
    this.capabilities = capabilities;
    this.length = length;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return capabilities;
  }

  @Override
  public int getLength()
  {
    return length;
  }

  @Override
  public BaseColumn getColumn()
  {
    return column;
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier()
  {
    return NoIndexesColumnIndexSupplier.getInstance();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public SettableColumnValueSelector makeNewSettableColumnValueSelector()
  {
    return ValueTypes.makeNewSettableColumnValueSelector(getCapabilities().getType());
  }
}
