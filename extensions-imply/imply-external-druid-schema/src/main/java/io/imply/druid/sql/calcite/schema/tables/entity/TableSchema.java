/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class TableSchema
{
  private final String name;
  private final List<TableColumn> columns;

  @JsonCreator
  public TableSchema(
      @JsonProperty("name") String name,
      @JsonProperty("columns") List<TableColumn> columns
  )
  {
    this.name = name;
    this.columns = columns;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<TableColumn> getColumns()
  {
    return columns;
  }

  @Override
  public String toString()
  {
    return "TableSchema{" +
           "name='" + name + '\'' +
           ", columns=" + columns +
           '}';
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
    TableSchema that = (TableSchema) o;
    return Objects.equals(name, that.name) && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, columns);
  }
}
