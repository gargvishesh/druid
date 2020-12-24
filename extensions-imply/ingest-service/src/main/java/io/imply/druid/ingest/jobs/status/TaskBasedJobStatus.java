/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.ingest.jobs.JobStatus;
import io.imply.druid.ingest.utils.StackTraceUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class TaskBasedJobStatus implements JobStatus
{
  private final String taskId;
  private final TaskStatusPlus taskStatus;
  private final TaskReport taskReport;
  private final IngestionStatsAndErrorsTaskReportData taskReportPayload;
  private final String userFacingMsg;

  @JsonCreator
  public TaskBasedJobStatus(
      @JsonProperty("taskId") @Nullable String taskId,
      @JsonProperty("status") @Nullable TaskStatusPlus taskStatus,
      @JsonProperty("taskReport") @Nullable TaskReport taskReport
  )
  {
    this.taskId = taskId;
    this.taskStatus = taskStatus;
    this.taskReport = taskReport;
    // The error in the taskReport contains a stack trace, remove it and get the message
    // only to avoid communicating internals to the user
    // An alternative to cleaning up would be to just store the message when
    // creating the task report in druid rather than the whole stack trace.
    // For now this is good enough to experiment with error handling in the
    // ingest service
    if (taskReport != null && taskReport.getPayload() instanceof IngestionStatsAndErrorsTaskReportData) {
      taskReportPayload = (IngestionStatsAndErrorsTaskReportData) taskReport.getPayload();
    } else {
      taskReportPayload = null;
    }
    userFacingMsg = computeMessage();
  }

  @JsonProperty("taskId")
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty("status")
  public TaskStatusPlus getTaskStatus()
  {
    return taskStatus;
  }

  @JsonProperty("taskReport")
  public TaskReport getTaskReport()
  {
    return taskReport;
  }

  @Override
  public String getUserFacingMessage()
  {
    return userFacingMsg;
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
    TaskBasedJobStatus that = (TaskBasedJobStatus) o;
    return Objects.equals(taskId, that.taskId) &&
           Objects.equals(taskStatus, that.taskStatus) &&
           Objects.equals(taskReport, that.taskReport);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, taskStatus, taskReport);
  }


  // todo: we could split this into a
  //  1) message payload with String content, and Enum type
  // were content is the message content and type one of {INFO, WARNING, ERROR}
  // for INFO & WARNING the message can be composed based on the stats in the
  // TaskReport
  // 2) A jobStats payload (Map<String,Object>) that is created from the task report stats
  // for now it is all put together in a structured String...
  private String computeMessage()
  {
    String message = null;
    if (taskReportPayload != null) {

      message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(taskReportPayload.getErrorMsg());

      Map determinePartitions = (Map) taskReportPayload.getRowStats().get("determinePartitions");
      Integer processed = (Integer) determinePartitions.get("processed");
      if (processed != null) {
        Integer processedWithError = (Integer) determinePartitions.get("processedWithError");
        Integer unparseable = (Integer) determinePartitions.get("unparseable");
        String processedMessage;
        if (message != null) {
          processedMessage = StringUtils.format(
              "Error: %s, rows processed: [%d], rows processed with errors: [%d]"
              + " rows unparseable: [%d].",
              message,
              processed,
              processedWithError,
              unparseable
          );
        } else {
          processedMessage = StringUtils.format(
              "Rows processed: [%d], rows processed with errors: [%d],"
              + " rows unparseable: [%d].",
              processed,
              processedWithError,
              unparseable
          );
        }
        if (processed == 0 && unparseable > 0) {
          processedMessage = StringUtils.format(
              "%s Hint: verify that the inputFormat in the job schema matches the input data.",
              processedMessage
          );
        } else if (processed == 0 && unparseable == 0) {
          processedMessage = StringUtils.format(
              "%s Hint: verify that the input file exists in the correct location.",
              processedMessage
          );
        }
        message = processedMessage;
      }
    }
    return message;
  }
}
