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
import org.apache.druid.indexer.TaskStatusPlus;

import javax.annotation.Nullable;
import java.util.Objects;

public class TaskBasedJobStatus implements JobStatus
{
  private final TaskStatusPlus taskStatus;

  @JsonCreator
  public TaskBasedJobStatus(
      @JsonProperty("status") @Nullable TaskStatusPlus taskStatus
  )
  {
    this.taskStatus = taskStatus;
  }

  @JsonProperty("status")
  public TaskStatusPlus getTaskStatus()
  {
    return taskStatus;
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
    return Objects.equals(taskStatus, that.taskStatus);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskStatus);
  }
}
