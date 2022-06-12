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

import java.util.Objects;

@JsonTypeName(InsertCannotOrderByDescendingFault.CODE)
public class InsertCannotOrderByDescendingFault extends BaseTalariaFault
{
  static final String CODE = "InsertCannotOrderByDescending";

  private final String columnName;

  @JsonCreator
  public InsertCannotOrderByDescendingFault(
      @JsonProperty("columnName") final String columnName
  )
  {
    super(CODE, "Cannot ingest column [%s] in descending order", columnName);
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
    InsertCannotOrderByDescendingFault that = (InsertCannotOrderByDescendingFault) o;
    return Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), columnName);
  }
}
