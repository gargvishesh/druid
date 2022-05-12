/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.query.QueryException;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Like {@link SqlAsyncQueryDetails}, but used for API responses. Has less information.
 */
public class SqlTaskStatus
{
  private final String taskId;
  private final TaskState state;
  @Nullable
  private final QueryException error;

  @JsonCreator
  public SqlTaskStatus(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("state") final TaskState state,
      @JsonProperty("error") @Nullable final QueryException error
  )
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.state = Preconditions.checkNotNull(state, "state");
    this.error = error;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public TaskState getState()
  {
    return state;
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
    SqlTaskStatus response = (SqlTaskStatus) o;
    return Objects.equals(taskId, response.taskId)
           && state == response.state
           && Objects.equals(error, response.error);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, state, error);
  }

  @Override
  public String toString()
  {
    return "SqlTaskStatus{" +
           "taskId='" + taskId + '\'' +
           ", state=" + state +
           ", error=" + error +
           '}';
  }
}
