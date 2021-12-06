/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;

public enum QueryWorkerInputType
{
  TABLE("table"),
  EXTERNAL("external"),
  SUBQUERY("query");

  private final String jsonString;

  QueryWorkerInputType(final String jsonString)
  {
    this.jsonString = jsonString;
  }

  @JsonCreator
  public static QueryWorkerInputType fromJsonString(final String jsonString)
  {
    for (final QueryWorkerInputType value : QueryWorkerInputType.values()) {
      if (value.jsonString.equals(jsonString)) {
        return value;
      }
    }

    throw new IAE("No such type [%s]", jsonString);
  }

  @Override
  @JsonValue
  public String toString()
  {
    return jsonString;
  }
}
