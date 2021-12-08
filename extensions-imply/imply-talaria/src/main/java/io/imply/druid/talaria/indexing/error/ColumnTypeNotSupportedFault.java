/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

@JsonTypeName(ColumnTypeNotSupportedFault.CODE)
public class ColumnTypeNotSupportedFault extends BaseTalariaFault
{
  static final String CODE = "ColumnTypeNotSupported";

  private final String columnName;

  @Nullable
  private final ColumnType columnType;

  @JsonCreator
  public ColumnTypeNotSupportedFault(
      @JsonProperty("columnName") final String columnName,
      @JsonProperty("columnType") @Nullable final ColumnType columnType
  )
  {
    super(CODE, UnsupportedColumnTypeException.message(columnName, columnType));
    this.columnName = Preconditions.checkNotNull(columnName, "columnName");
    this.columnType = columnType;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ColumnType getColumnType()
  {
    return columnType;
  }
}
