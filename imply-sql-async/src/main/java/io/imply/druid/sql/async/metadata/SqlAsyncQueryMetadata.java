/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.metadata;

public class SqlAsyncQueryMetadata
{
  // The time in milliseconds from epoch when Sql Async Query was last updated.

  private final long lastUpdatedTime;


  public SqlAsyncQueryMetadata(
      long lastUpdatedTime
  )
  {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  public long getLastUpdatedTime()
  {
    return lastUpdatedTime;
  }
}
