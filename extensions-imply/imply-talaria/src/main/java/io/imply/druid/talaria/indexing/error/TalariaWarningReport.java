/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Objects;

public class TalariaWarningReport
{
  private final String taskId;
  @Nullable
  private final String host;
  @Nullable
  private final Integer stageNumber;
  private final String warning;
  @Nullable
  private final String exceptionStackTrace;
  private final Integer priority;

  @JsonCreator
  public TalariaWarningReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("host") @Nullable final String host,
      @JsonProperty("stageNumber") final Integer stageNumber,
      @JsonProperty("warning") final String warning,
      @JsonProperty("exceptionStackTrace") @Nullable final String exceptionStackTrace,
      @JsonProperty("priority") final Integer priority
  )
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.host = host;
    this.stageNumber = stageNumber;
    this.warning = Preconditions.checkNotNull(warning, "warning");
    this.exceptionStackTrace = exceptionStackTrace;
    this.priority = Preconditions.checkNotNull(priority, "priority");
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getHost()
  {
    return host;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty("warning")
  public String getFault()
  {
    return warning;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExceptionStackTrace()
  {
    return exceptionStackTrace;
  }

  @JsonProperty
  public Integer priority()
  {
    return priority;
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
    TalariaWarningReport that = (TalariaWarningReport) o;
    return Objects.equals(taskId, that.taskId)
           && Objects.equals(host, that.host)
           && Objects.equals(stageNumber, that.stageNumber)
           && Objects.equals(warning, that.warning)
           && Objects.equals(exceptionStackTrace, that.exceptionStackTrace)
           && Objects.equals(priority, that.priority);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, host, warning, exceptionStackTrace, priority);
  }

  @Override
  public String toString()
  {
    return "TalariaWarningReport{" +
           "taskId='" + taskId + '\'' +
           ", host='" + host + '\'' +
           ", stageNumber=" + stageNumber +
           ", warning=" + warning +
           ", priority='" + priority + '\'' +
           ", exceptionStackTrace='" + exceptionStackTrace + '\'' +
           '}';
  }
}
