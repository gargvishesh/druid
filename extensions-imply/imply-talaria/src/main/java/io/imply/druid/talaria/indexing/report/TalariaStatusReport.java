/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import org.apache.druid.indexer.TaskState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class TalariaStatusReport
{
  private final TaskState status;

  @Nullable
  private final TalariaErrorReport errorReport;

  @Nullable
  private final DateTime startTime;

  private final long durationMs;


  @JsonCreator
  public TalariaStatusReport(
      @JsonProperty("status") TaskState status,
      @JsonProperty("error") @Nullable TalariaErrorReport errorReport,
      @JsonProperty("startTime") @Nullable DateTime startTime,
      @JsonProperty("durationMs") long durationMs
  )
  {
    this.status = Preconditions.checkNotNull(status, "status");
    this.errorReport = errorReport;
    this.startTime = Preconditions.checkNotNull(startTime, "startTime");
    this.durationMs = durationMs;
  }

  @JsonProperty
  public TaskState getStatus()
  {
    return status;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public TalariaErrorReport getErrorReport()
  {
    return errorReport;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DateTime getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public long getDurationMs()
  {
    return durationMs;
  }
}
