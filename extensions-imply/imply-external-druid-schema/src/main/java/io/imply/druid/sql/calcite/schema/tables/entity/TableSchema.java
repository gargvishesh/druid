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
  private final TableSchemaMode schemaMode;

  @JsonCreator
  public TableSchema(
      @JsonProperty("name") String name,
      @JsonProperty("columns") List<TableColumn> columns,
      @JsonProperty("schemaMode") TableSchemaMode schemaMode
  )
  {
    this.name = name;
    this.columns = columns;
    this.schemaMode = schemaMode == null ? TableSchemaMode.STRICT : schemaMode;
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

  @JsonProperty
  public TableSchemaMode getSchemaMode()
  {
    return schemaMode;
  }

  @Override
  public String toString()
  {
    return "TableSchema{" +
           "name='" + name + '\'' +
           ", columns=" + columns +
           ", schemaMode=" + schemaMode +
           '}';
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableSchema that = (TableSchema) o;
    return Objects.equals(getName(), that.getName()) && Objects.equals(
        getColumns(),
        that.getColumns()
    ) && getSchemaMode() == that.getSchemaMode();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getName(), getColumns(), getSchemaMode());
  }
}
