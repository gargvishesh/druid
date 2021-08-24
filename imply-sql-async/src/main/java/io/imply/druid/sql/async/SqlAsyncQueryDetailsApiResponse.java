/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.QueryException;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;

/**
 * Like {@link SqlAsyncQueryDetails}, but used for API responses. Has less information.
 */
public class SqlAsyncQueryDetailsApiResponse
{
  private final String sqlQueryId;
  private final SqlAsyncQueryDetails.State state;
  @Nullable
  private final ResultFormat resultFormat;
  private final long resultLength;
  @Nullable
  private final QueryException error;

  public SqlAsyncQueryDetailsApiResponse(
      final String sqlQueryId,
      final SqlAsyncQueryDetails.State state,
      @Nullable final ResultFormat resultFormat,
      final long resultLength,
      @Nullable final QueryException error
  )
  {
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.state = Preconditions.checkNotNull(state, "state");
    this.resultFormat = resultFormat;
    this.resultLength = resultLength;
    this.error = error;
  }

  @JsonProperty
  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  @JsonProperty
  public SqlAsyncQueryDetails.State getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getResultLength()
  {
    return resultLength;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public QueryException getError()
  {
    return error;
  }

  @Override
  public String toString()
  {
    return "SqlAsyncQueryDetailsApiResponse{" +
           "sqlQueryId='" + sqlQueryId + '\'' +
           ", state=" + state +
           ", resultLength=" + resultLength +
           ", resultFormat=" + resultFormat +
           ", error=" + error +
           '}';
  }
}
