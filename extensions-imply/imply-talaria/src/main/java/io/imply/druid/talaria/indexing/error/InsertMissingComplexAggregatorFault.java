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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.column.ColumnType;

@JsonTypeName(InsertMissingComplexAggregatorFault.CODE)
public class InsertMissingComplexAggregatorFault extends BaseTalariaFault
{
  static final String CODE = "InsertMissingComplexAggregator";

  private final String columnName;
  private final ColumnType columnType;

  @JsonCreator
  public InsertMissingComplexAggregatorFault(
      @JsonProperty("columnName") final String columnName,
      @JsonProperty("columnType") final ColumnType columnType
  )
  {
    super(
        CODE,
        "Cannot ingest complex column [%s] of type [%s] without a matching aggregator; try using a group-by",
        columnName,
        columnType
    );

    this.columnName = Preconditions.checkNotNull(columnName, "columnName");
    this.columnType = Preconditions.checkNotNull(columnType, "columnType");
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public ColumnType getColumnType()
  {
    return columnType;
  }
}
