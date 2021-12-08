/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class ColumnMapping
{
  private final String queryColumn;
  private final String outputColumn;

  @JsonCreator
  public ColumnMapping(
      @JsonProperty("queryColumn") String queryColumn,
      @JsonProperty("outputColumn") String outputColumn
  )
  {
    this.queryColumn = Preconditions.checkNotNull(queryColumn, "queryColumn");
    this.outputColumn = Preconditions.checkNotNull(outputColumn, "outputColumn");
  }

  @JsonProperty
  public String getQueryColumn()
  {
    return queryColumn;
  }

  @JsonProperty
  public String getOutputColumn()
  {
    return outputColumn;
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
    ColumnMapping that = (ColumnMapping) o;
    return Objects.equals(queryColumn, that.queryColumn) && Objects.equals(outputColumn, that.outputColumn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryColumn, outputColumn);
  }

  @Override
  public String toString()
  {
    return "ColumnMapping{" +
           "queryColumn='" + queryColumn + '\'' +
           ", outputColumn='" + outputColumn + '\'' +
           '}';
  }
}
