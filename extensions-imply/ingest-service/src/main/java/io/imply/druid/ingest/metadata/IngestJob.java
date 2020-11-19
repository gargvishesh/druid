/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.JobStatus;
import org.joda.time.DateTime;

// todo: this probably needs JSON stuff eventually, and maybe should be less mutable
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

  public IngestJob(
      String tableName,
      String jobId,
      JobState jobState
  )
  {
    this.tableName = tableName;
    this.jobId = jobId;
    this.jobState = jobState;
  }

  public String getTableName()
  {
    return tableName;
  }

  public String getJobId()
  {
    return jobId;
  }

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
}
