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
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

public class SqlAsyncQueryDetails
{
  @Nullable // Can only be null if state == ERROR
  private final String sqlQueryId;

  @Nullable // Can only be null if state == ERROR
  private final String identity;
  private final State state;

  @Nullable
  private final String errorMessage;

  @JsonCreator
  private SqlAsyncQueryDetails(
      @JsonProperty("sqlQueryId") @Nullable final String sqlQueryId,
      @JsonProperty("identity") @Nullable final String identity,
      @JsonProperty("state") final State state,
      @JsonProperty("error") @Nullable final String errorMessage
  )
  {
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");

    if (sqlQueryId.isEmpty()) {
      throw new IAE("sqlQueryId must be nonempty");
    }

    this.identity = identity;
    this.state = Preconditions.checkNotNull(state, "state");
    this.errorMessage = errorMessage;

    if (state != State.ERROR && sqlQueryId == null) {
      throw new ISE("Cannot have nil sqlQueryId in non-error state");
    }

    if (state != State.ERROR && identity == null) {
      throw new ISE("Cannot have nil identity in non-error state");
    }

    if (state != State.ERROR && errorMessage != null) {
      throw new ISE("Cannot have error message in state [%s]", state);
    }
  }

  public static SqlAsyncQueryDetails createNew(final String sqlQueryId, final String identity)
  {
    return new SqlAsyncQueryDetails(sqlQueryId, identity, State.INITIALIZED, null);
  }

  public static SqlAsyncQueryDetails createError(
      @Nullable final String sqlQueryId,
      @Nullable final String identity,
      @Nullable final Throwable e
  )
  {
    final StringBuilder errorMessage = new StringBuilder();
    Throwable current = e;

    while (current != null) {
      errorMessage.append(current);
      current = e.getCause();
      if (current != null) {
        errorMessage.append("; ");
      }
    }

    return new SqlAsyncQueryDetails(
        sqlQueryId,
        identity,
        State.ERROR,
        StringUtils.emptyToNullNonDruidDataString(errorMessage.toString())
    );
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getIdentity()
  {
    return identity;
  }

  @JsonProperty
  public State getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMessage()
  {
    return errorMessage;
  }

  public SqlAsyncQueryDetails withState(final State newState)
  {
    return new SqlAsyncQueryDetails(sqlQueryId, identity, newState, errorMessage);
  }

  public SqlAsyncQueryDetails withException(@Nullable final Exception e)
  {
    return createError(getSqlQueryId(), getIdentity(), e);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqlAsyncQueryDetails that = (SqlAsyncQueryDetails) o;
    return Objects.equals(sqlQueryId, that.sqlQueryId)
           && Objects.equals(identity, that.identity)
           && state == that.state
           && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sqlQueryId, identity, state, errorMessage);
  }

  public enum State
  {
    INITIALIZED,
    RUNNING,
    COMPLETE,
    ERROR
  }
}
