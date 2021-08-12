/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
