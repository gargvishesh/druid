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

package org.apache.druid.sql.async;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;

public class SqlAsyncQueryDetails
{
  private static final int MAX_ERROR_MESSAGE_LENGTH = 50_000;
  private static final long UNKNOWN_RESULT_LENGTH = 0;

  private final String sqlQueryId;
  private final State state;
  private final ResultFormat resultFormat;
  private final long resultLength;

  @Nullable // Can only be null if state == ERROR
  private final String identity;

  @Nullable
  private final QueryException error;

  @JsonCreator
  private SqlAsyncQueryDetails(
      @JsonProperty("sqlQueryId") final String sqlQueryId,
      @JsonProperty("state") final State state,
      @JsonProperty("identity") @Nullable final String identity,
      @JsonProperty("resultFormat") final ResultFormat resultFormat,
      @JsonProperty("resultLength") final long resultLength,
      @JsonProperty("error") @Nullable final QueryException error
  )
  {
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.state = Preconditions.checkNotNull(state, "state");
    this.identity = identity;
    this.resultFormat = Preconditions.checkNotNull(resultFormat, "resultFormat");
    this.resultLength = resultLength;
    this.error = error;

    if (sqlQueryId.isEmpty()) {
      throw new IAE("sqlQueryId must be nonempty");
    }

    if (state != State.ERROR && identity == null) {
      throw new ISE("Cannot have nil identity in state [%s]", state);
    }

    if (state != State.ERROR && error != null) {
      throw new ISE("Cannot have error details in state [%s]", state);
    }

    if (state != State.COMPLETE && resultLength != UNKNOWN_RESULT_LENGTH) {
      throw new ISE("Cannot have result length in state [%s]", state);
    }
  }

  public static SqlAsyncQueryDetails createNew(
      final String sqlQueryId,
      final String identity,
      final ResultFormat resultFormat
  )
  {
    return new SqlAsyncQueryDetails(sqlQueryId, State.INITIALIZED, identity, resultFormat, UNKNOWN_RESULT_LENGTH, null);
  }

  public static SqlAsyncQueryDetails createError(
      final String sqlQueryId,
      @Nullable final String identity,
      final ResultFormat resultFormat,
      @Nullable final Throwable e
  )
  {
    return new SqlAsyncQueryDetails(
        sqlQueryId,
        State.ERROR,
        identity,
        resultFormat,
        UNKNOWN_RESULT_LENGTH,
        // Don't use QueryInterruptedException.wrapIfNeeded, because we want to leave regular QueryException unwrapped
        e instanceof QueryException ? (QueryException) e : new QueryInterruptedException(e)
    );
  }

  @JsonProperty
  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  @JsonProperty
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public long getResultLength()
  {
    return resultLength;
  }

  @JsonProperty
  public State getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getIdentity()
  {
    // TODO(gianm): Include in ZK metadata, but not in API response. Two different objects?
    return identity;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public QueryException getError()
  {
    return error;
  }

  public SqlAsyncQueryDetails toRunning()
  {
    return new SqlAsyncQueryDetails(sqlQueryId, State.RUNNING, identity, resultFormat, resultLength, error);
  }

  public SqlAsyncQueryDetails toComplete(final long newResultLength)
  {
    return new SqlAsyncQueryDetails(sqlQueryId, State.COMPLETE, identity, resultFormat, newResultLength, error);
  }

  public SqlAsyncQueryDetails toError(@Nullable final Throwable e)
  {
    return createError(sqlQueryId, identity, resultFormat, e);
  }

  public SqlAsyncQueryDetailsApiResponse toApiResponse()
  {
    return new SqlAsyncQueryDetailsApiResponse(
        sqlQueryId,
        state,
        state == State.COMPLETE ? resultFormat : null,
        resultLength,
        error
    );
  }

  public enum State
  {
    INITIALIZED,
    RUNNING,
    COMPLETE,
    ERROR
  }

  private static String clipErrorMessage(@Nullable final String errorMessage)
  {
    if (errorMessage != null && errorMessage.length() > MAX_ERROR_MESSAGE_LENGTH) {
      return errorMessage.substring(0, MAX_ERROR_MESSAGE_LENGTH - 3) + "...";
    } else {
      return errorMessage;
    }
  }
}
