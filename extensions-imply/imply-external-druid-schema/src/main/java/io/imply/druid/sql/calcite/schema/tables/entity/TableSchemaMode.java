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
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The table's schema enforcement mode.
 */
public enum TableSchemaMode
{

  FLEXIBLE("flexible"),

  STRICT("strict");

  private String value;

  TableSchemaMode(String value)
  {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return String.valueOf(value);
  }

  @JsonCreator
  public static TableSchemaMode fromValue(String value)
  {
    for (TableSchemaMode b : TableSchemaMode.values()) {
      if (String.valueOf(b.value).replace('-', '_').equalsIgnoreCase(String.valueOf(value).replace('-', '_'))) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}


