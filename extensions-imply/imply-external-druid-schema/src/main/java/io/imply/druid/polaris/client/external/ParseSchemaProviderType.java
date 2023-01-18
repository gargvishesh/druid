/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Data format:   * &#x60;connection&#x60; - Retrieves the parse schema from an external source defined by a connection. # docs-internal   * &#x60;inline-avro&#x60; - An inline Avro schema.   * &#x60;inline-protobuf&#x60; - An inline compiled Protobuf descriptor, encoded as a Base64 string.
 */
public enum ParseSchemaProviderType
{

  CONNECTION("connection"),

  INLINE_AVRO("inline-avro"),

  INLINE_PROTOBUF("inline-protobuf");

  private String value;

  public String value()
  {
    return value;
  }

  ParseSchemaProviderType(String value)
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
  public static ParseSchemaProviderType fromValue(String value)
  {
    for (ParseSchemaProviderType b : ParseSchemaProviderType.values()) {
      if (String.valueOf(b.value).replace('-', '_').equalsIgnoreCase(String.valueOf(value).replace('-', '_'))) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
