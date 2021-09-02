/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.QueryException;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Like {@link SqlAsyncQueryDetails}, but used for API responses. Has less information.
 */
public class SqlAsyncQueryDetailsApiResponse
{
  private final String asyncResultId;
  private final SqlAsyncQueryDetails.State state;
  @Nullable
  private final ResultFormat resultFormat;
  private final long resultLength;
  @Nullable
  private final QueryException error;

  @JsonCreator
  public SqlAsyncQueryDetailsApiResponse(
      @JsonProperty("asyncResultId") final String asyncResultId,
      @JsonProperty("state") final SqlAsyncQueryDetails.State state,
      @JsonProperty("resultFormat") @Nullable final ResultFormat resultFormat,
      @JsonProperty("resultLength") final long resultLength,
      @JsonProperty("error") @Nullable final QueryException error
  )
  {
    this.asyncResultId = Preconditions.checkNotNull(asyncResultId, "asyncResultId");
    this.state = Preconditions.checkNotNull(state, "state");
    this.resultFormat = resultFormat;
    this.resultLength = resultLength;
    this.error = error;
  }

  @JsonProperty
  public String getAsyncResultId()
  {
    return asyncResultId;
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) o;
    return resultLength == response.resultLength
           && Objects.equals(asyncResultId, response.asyncResultId)
           && state == response.state
           && resultFormat == response.resultFormat
           && Objects.equals(error, response.error);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(asyncResultId, state, resultFormat, resultLength, error);
  }

  @Override
  public String toString()
  {
    return "SqlAsyncQueryDetailsApiResponse{" +
           "asyncResultId='" + asyncResultId + '\'' +
           ", state=" + state +
           ", resultLength=" + resultLength +
           ", resultFormat=" + resultFormat +
           ", error=" + error +
           '}';
  }
}
