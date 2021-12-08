/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public class UnsupportedColumnTypeException extends RuntimeException
{
  private final String columnName;
  @Nullable
  private final ColumnType columnType;

  UnsupportedColumnTypeException(final String columnName, @Nullable final ColumnType columnType)
  {
    super(message(columnName, columnType));
    this.columnName = columnName;
    this.columnType = columnType;
  }

  public static String message(final String columnName, @Nullable final ColumnType columnType)
  {
    return columnType == null
           ? StringUtils.format("Cannot handle olumn [%s] with unknown type", columnName)
           : StringUtils.format("Cannot handle column [%s] with type [%s]", columnName, columnType);
  }

  public String getColumnName()
  {
    return columnName;
  }

  @Nullable
  public ColumnType getColumnType()
  {
    return columnType;
  }
}
