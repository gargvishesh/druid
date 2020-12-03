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

import java.util.Objects;

// todo: maybe this should be less mutable
public class IngestJob
{
  private final String tableName;
  private final String jobId;

  private JobState jobState;
  private JobStatus jobStatus;
  private JobRunner jobRunner;
  private Long schemaId;
  private IngestSchema schema;
  private DateTime createdTime;
  private DateTime scheduledTime;
  private DateTime taskCreatedTime;
  private int retryCount;

  @JsonCreator
  public IngestJob(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("jobId") String jobId,
      @JsonProperty("jobState") JobState jobState
  )
  {
    this.tableName = tableName;
    this.jobId = jobId;
    this.jobState = jobState;
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

  public IngestJob setJobState(JobState jobState)
  {
    this.jobState = jobState;
    return this;
  }

  public Long getSchemaId()
  {
    return schemaId;
  }

  public IngestJob setSchemaId(Long schemaId)
  {
    this.schemaId = schemaId;
    return this;
  }

  public IngestSchema getSchema()
  {
    return schema;
  }

  public IngestJob setSchema(IngestSchema schema)
  {
    this.schema = schema;
    return this;
  }

  public JobStatus getJobStatus()
  {
    return jobStatus;
  }

  public IngestJob setJobStatus(JobStatus jobStatus)
  {
    this.jobStatus = jobStatus;
    return this;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public IngestJob setCreatedTime(DateTime createdTime)
  {
    this.createdTime = createdTime;
    return this;
  }

  public DateTime getScheduledTime()
  {
    return scheduledTime;
  }

  public IngestJob setScheduledTime(DateTime scheduledTime)
  {
    this.scheduledTime = scheduledTime;
    return this;
  }

  public DateTime getTaskCreatedTime()
  {
    return taskCreatedTime;
  }

  public IngestJob setTaskCreatedTime(DateTime taskCreatedTime)
  {
    this.taskCreatedTime = taskCreatedTime;
    return this;
  }

  public int getRetryCount()
  {
    return retryCount;
  }

  public IngestJob setRetryCount(int retryCount)
  {
    this.retryCount = retryCount;
    return this;
  }

  public IngestJob incrementRetry()
  {
    this.retryCount++;
    return this;
  }

  public JobRunner getJobRunner()
  {
    return jobRunner;
  }

  public IngestJob setJobRunner(JobRunner jobRunner)
  {
    this.jobRunner = jobRunner;
    return this;
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
    return getRetryCount() == ingestJob.getRetryCount() &&
           Objects.equals(getTableName(), ingestJob.getTableName()) &&
           Objects.equals(getJobId(), ingestJob.getJobId()) &&
           getJobState() == ingestJob.getJobState() &&
           Objects.equals(getJobStatus(), ingestJob.getJobStatus()) &&
           Objects.equals(getSchemaId(), ingestJob.getSchemaId()) &&
           Objects.equals(getSchema(), ingestJob.getSchema()) &&
           Objects.equals(getCreatedTime(), ingestJob.getCreatedTime()) &&
           Objects.equals(getScheduledTime(), ingestJob.getScheduledTime()) &&
           Objects.equals(getTaskCreatedTime(), ingestJob.getTaskCreatedTime());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getTableName(),
        getJobId(),
        getJobState(),
        getJobStatus(),
        getSchemaId(),
        getSchema(),
        getCreatedTime(),
        getScheduledTime(),
        getTaskCreatedTime(),
        getRetryCount()
    );
  }
}
