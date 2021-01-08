/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class TablesResponse
{
  private final List<TableInfo> tables;

  @JsonCreator
  public TablesResponse(
      @JsonProperty("tables") List<TableInfo> tables
  )
  {
    this.tables = tables;
  }

  @JsonProperty
  public List<TableInfo> getTables()
  {
    return tables;
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
    TablesResponse that = (TablesResponse) o;
    return Objects.equals(tables, that.tables);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tables);
  }

  @Override
  public String toString()
  {
    return "TablesResponse{" +
           "tables=" + tables +
           '}';
  }
}
