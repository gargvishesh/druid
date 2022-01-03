/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Deprecated only used to maintain backwards compatability.
 */
public class SqlAsyncQueryMetadata
{
  // The time in milliseconds from epoch when Sql Async Query was last accessed or updated.

  private final long lastUpdatedTime;

  @JsonCreator
  public SqlAsyncQueryMetadata(
      @JsonProperty("lastUpdatedTime") long lastUpdatedTime
  )
  {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  @JsonProperty
  public long getLastUpdatedTime()
  {
    return lastUpdatedTime;
  }
}
