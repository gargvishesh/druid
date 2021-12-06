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
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;

public enum TalariaCounterType
{
  INPUT("input"),
  PRESHUFFLE("preShuffle"),
  OUTPUT("output");

  private final String key;

  TalariaCounterType(String key)
  {
    this.key = key;
  }

  @JsonCreator
  public static TalariaCounterType fromString(final String key)
  {
    for (final TalariaCounterType counterType : values()) {
      if (counterType.key().equals(key)) {
        return counterType;
      }
    }

    throw new IAE("No such counterType [%s]", key);
  }

  public String key()
  {
    return key;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return key;
  }
}
