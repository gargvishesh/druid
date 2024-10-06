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

package org.apache.druid.msq.dart.controller.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.util.MSQTaskQueryMakerUtils;
import org.apache.druid.query.QueryContexts;
import org.joda.time.DateTime;

import java.util.Objects;

/**
 * Class included in {@link GetQueriesResponse}.
 */
public class DartQueryInfo
{
  private final String sqlQueryId;
  private final String dartQueryId;
  private final String sql;
  private final String authenticator;
  private final String identity;
  private final DateTime startTime;
  private final String state;

  @JsonCreator
  public DartQueryInfo(
      @JsonProperty("sqlQueryId") final String sqlQueryId,
      @JsonProperty("dartQueryId") final String dartQueryId,
      @JsonProperty("sql") final String sql,
      @JsonProperty("authenticator") final String authenticator,
      @JsonProperty("identity") final String identity,
      @JsonProperty("startTime") final DateTime startTime,
      @JsonProperty("state") final String state
  )
  {
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.dartQueryId = Preconditions.checkNotNull(dartQueryId, "dartQueryId");
    this.sql = sql;
    this.authenticator = authenticator;
    this.identity = identity;
    this.startTime = startTime;
    this.state = state;
  }

  public static DartQueryInfo fromControllerHolder(final ControllerHolder holder)
  {
    return new DartQueryInfo(
        holder.getSqlQueryId(),
        holder.getController().queryId(),
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(holder.getSql()),
        holder.getAuthenticationResult().getAuthenticatedBy(),
        holder.getAuthenticationResult().getIdentity(),
        holder.getStartTime(),
        holder.getState().toString()
    );
  }

  /**
   * The {@link QueryContexts#CTX_SQL_QUERY_ID} provided by the user, or generated by the system.
   */
  @JsonProperty
  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  /**
   * Dart query ID generated by the system. Globally unique.
   */
  @JsonProperty
  public String getDartQueryId()
  {
    return dartQueryId;
  }

  /**
   * SQL string for this query, masked using {@link MSQTaskQueryMakerUtils#maskSensitiveJsonKeys(String)}.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSql()
  {
    return sql;
  }

  /**
   * Authenticator that authenticated the identity from {@link #getIdentity()}.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAuthenticator()
  {
    return authenticator;
  }

  /**
   * User that issued this query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getIdentity()
  {
    return identity;
  }

  /**
   * Time this query was started.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DateTime getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public String getState()
  {
    return state;
  }

  /**
   * Returns a copy of this instance with {@link #getAuthenticator()} and {@link #getIdentity()} nulled.
   */
  public DartQueryInfo withoutAuthenticationResult()
  {
    return new DartQueryInfo(sqlQueryId, dartQueryId, sql, null, null, startTime, state);
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
    DartQueryInfo that = (DartQueryInfo) o;
    return Objects.equals(sqlQueryId, that.sqlQueryId)
           && Objects.equals(dartQueryId, that.dartQueryId)
           && Objects.equals(sql, that.sql)
           && Objects.equals(authenticator, that.authenticator)
           && Objects.equals(identity, that.identity)
           && Objects.equals(startTime, that.startTime)
           && Objects.equals(state, that.state);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sqlQueryId, dartQueryId, sql, authenticator, identity, startTime, state);
  }

  @Override
  public String toString()
  {
    return "DartQueryInfo{" +
           "sqlQueryId='" + sqlQueryId + '\'' +
           ", dartQueryId='" + dartQueryId + '\'' +
           ", sql='" + sql + '\'' +
           ", authenticator='" + authenticator + '\'' +
           ", identity='" + identity + '\'' +
           ", startTime=" + startTime +
           ", state=" + state +
           '}';
  }
}
