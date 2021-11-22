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
import org.apache.druid.segment.column.ColumnType;

import java.util.Objects;

public class TableColumn
{
  private final String name;
  private final ColumnType type;

  @JsonCreator
  public TableColumn(
      @JsonProperty("name") String name,
      @JsonProperty("type") ColumnType type
  )
  {
    this.name = name;
    this.type = type;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public ColumnType getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "TableColumn{" +
           "name='" + name + '\'' +
           ", type=" + type +
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
    TableColumn that = (TableColumn) o;
    return Objects.equals(name, that.name) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, type);
  }
}
