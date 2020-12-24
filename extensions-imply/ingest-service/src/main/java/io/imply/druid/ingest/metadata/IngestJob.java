/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.JobStatus;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class IngestJob
{
  private final String tableName;
  private final String jobId;

  private final JobState jobState;
  private final JobStatus jobStatus;
  private final JobRunner jobRunner;
  private final IngestSchema schema;
  private final DateTime createdTime;
  private final DateTime scheduledTime;
  private final int retryCount;

  @JsonCreator
  public IngestJob(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("jobId") String jobId,
      @JsonProperty("jobState") JobState jobState,
      @JsonProperty("jobStatus") @Nullable JobStatus jobStatus,
      @JsonProperty("jobRunner") @Nullable JobRunner jobRunner,
      @JsonProperty("schema") @Nullable IngestSchema schema,
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("scheduledTime") @Nullable DateTime scheduledTime,
      @JsonProperty("retryCount") @Nullable int retryCount
  )
  {
    this.tableName = tableName;
    this.jobId = jobId;
    this.jobState = jobState;
    this.jobStatus = jobStatus;
    this.jobRunner = jobRunner;
    this.schema = schema;
    this.createdTime = createdTime;
    this.scheduledTime = scheduledTime;
    this.retryCount = retryCount;
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  @JsonProperty
  public String getJobId()
  {
    return jobId;
  }

  @JsonProperty
  public JobState getJobState()
  {
    return jobState;
  }

  @JsonProperty
  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @JsonProperty
  public DateTime getScheduledTime()
  {
    return scheduledTime;
  }

  @JsonProperty("message")
  @Nullable
  public String getUserFacingMessage()
  {
    return Optional.ofNullable(jobStatus).map(JobStatus::getUserFacingMessage).orElse(null);
  }

  public IngestSchema getSchema()
  {
    return schema;
  }

  public JobStatus getJobStatus()
  {
    return jobStatus;
  }

  public JobRunner getJobRunner()
  {
    return jobRunner;
  }

  public int getRetryCount()
  {
    return retryCount;
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
    IngestJob ingestJob = (IngestJob) o;
    return retryCount == ingestJob.retryCount &&
           Objects.equals(tableName, ingestJob.tableName) &&
           Objects.equals(jobId, ingestJob.jobId) &&
           jobState == ingestJob.jobState &&
           Objects.equals(jobStatus, ingestJob.jobStatus) &&
           Objects.equals(jobRunner, ingestJob.jobRunner) &&
           Objects.equals(schema, ingestJob.schema) &&
           Objects.equals(createdTime, ingestJob.createdTime) &&
           Objects.equals(scheduledTime, ingestJob.scheduledTime);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        tableName,
        jobId,
        jobState,
        jobStatus,
        jobRunner,
        schema,
        createdTime,
        scheduledTime,
        retryCount
    );
  }

  @Override
  public String toString()
  {
    return "IngestJob{" +
           "tableName='" + tableName + '\'' +
           ", jobId='" + jobId + '\'' +
           ", jobState=" + jobState +
           ", jobStatus=" + jobStatus +
           ", jobRunner=" + jobRunner +
           ", schema=" + schema +
           ", createdTime=" + createdTime +
           ", scheduledTime=" + scheduledTime +
           ", retryCount=" + retryCount +
           '}';
  }
}
