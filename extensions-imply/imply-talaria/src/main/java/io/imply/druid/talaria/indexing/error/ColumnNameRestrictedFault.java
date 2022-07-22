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
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

@JsonTypeName(ColumnNameRestrictedFault.CODE)
public class ColumnNameRestrictedFault extends BaseTalariaFault
{
  static final String CODE = "ColumnNameRestricted";

  private final String columnName;

  @JsonCreator
  public ColumnNameRestrictedFault(
      @JsonProperty("columnName") final String columnName
  )
  {
    super(CODE, StringUtils.format(
        "[%s] column name is reserved for MSQ's internal purpose. Please retry the query after renaming the column.",
        columnName));
    this.columnName = Preconditions.checkNotNull(columnName, "columnName");
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
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
    if (!super.equals(o)) {
      return false;
    }
    ColumnNameRestrictedFault that = (ColumnNameRestrictedFault) o;
    return Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), columnName);
  }
}
