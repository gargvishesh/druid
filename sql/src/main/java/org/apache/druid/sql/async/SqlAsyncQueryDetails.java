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

  private final String sqlQueryId;
  private final ResultFormat resultFormat;
  private final State state;

  @Nullable // Can only be null if state == ERROR
  private final String identity;

  @Nullable
  private final QueryException error;

  @JsonCreator
  private SqlAsyncQueryDetails(
      @JsonProperty("sqlQueryId") final String sqlQueryId,
      @JsonProperty("resultFormat") final ResultFormat resultFormat,
      @JsonProperty("state") final State state,
      @JsonProperty("identity") @Nullable final String identity,
      @JsonProperty("error") @Nullable final QueryException error
  )
  {
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.resultFormat = Preconditions.checkNotNull(resultFormat, "resultFormat");
    this.state = Preconditions.checkNotNull(state, "state");
    this.identity = identity;
    this.error = error;

    if (sqlQueryId.isEmpty()) {
      throw new IAE("sqlQueryId must be nonempty");
    }

    if (state != State.ERROR && identity == null) {
      throw new ISE("Cannot have nil identity in non-error state");
    }

    if (state != State.ERROR && error != null) {
      throw new ISE("Cannot have error details in state [%s]", state);
    }
  }

  public static SqlAsyncQueryDetails createNew(
      final String sqlQueryId,
      final ResultFormat resultFormat,
      final String identity
  )
  {
    return new SqlAsyncQueryDetails(sqlQueryId, resultFormat, State.INITIALIZED, identity, null);
  }

  public static SqlAsyncQueryDetails createError(
      final String sqlQueryId,
      final ResultFormat resultFormat,
      @Nullable final String identity,
      @Nullable final Exception e
  )
  {
    return new SqlAsyncQueryDetails(
        sqlQueryId,
        resultFormat,
        State.ERROR,
        identity,
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
  public State getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getIdentity()
  {
    return identity;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public QueryException getError()
  {
    return error;
  }

  public SqlAsyncQueryDetails withState(final State newState)
  {
    return new SqlAsyncQueryDetails(sqlQueryId, resultFormat, newState, identity, error);
  }

  public SqlAsyncQueryDetails withException(@Nullable final Exception e)
  {
    return createError(sqlQueryId, resultFormat, identity, e);
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
