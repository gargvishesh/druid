/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.query;

import io.imply.druid.sql.async.metadata.SqlAsyncQueryMetadata;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class SqlAsyncQueryDetailsAndMetadata
{
  private final SqlAsyncQueryDetails sqlAsyncQueryDetails;
  private final SqlAsyncQueryMetadata metadata;

  public SqlAsyncQueryDetailsAndMetadata(
      @NotNull SqlAsyncQueryDetails sqlAsyncQueryDetails,
      @Nullable SqlAsyncQueryMetadata metadata
  )
  {
    this.sqlAsyncQueryDetails = sqlAsyncQueryDetails;
    this.metadata = metadata;
  }

  @NotNull
  public SqlAsyncQueryDetails getSqlAsyncQueryDetails()
  {
    return sqlAsyncQueryDetails;
  }

  @Nullable
  public SqlAsyncQueryMetadata getMetadata()
  {
    return metadata;
  }
}
