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

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * Materialized object form of job table data stored in the jobs table of the {@link IngestServiceMetadataStore}.
 * {@link IngestSchema} will be created either from inline stored blob, or from
 * {@link IngestServiceMetadataStore#getSchema} if the row refers to an external schema.
 */
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

  public IngestJob(
      String tableName,
      String jobId,
      JobState jobState,
      @Nullable JobStatus jobStatus,
      @Nullable JobRunner jobRunner,
      @Nullable IngestSchema schema,
      DateTime createdTime,
      @Nullable DateTime scheduledTime,
      @Nullable int retryCount
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

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public DateTime getScheduledTime()
  {
    return scheduledTime;
  }

  @Nullable
  public String getUserFacingMessage()
  {
    return Optional.ofNullable(jobStatus).map(JobStatus::getMessage).orElse(null);
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
