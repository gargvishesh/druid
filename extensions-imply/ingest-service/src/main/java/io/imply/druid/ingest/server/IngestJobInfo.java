/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;

public class IngestJobInfo
{
  private final String tableName;
  private final String jobId;

  private final JobState jobState;
  private final IngestSchema schema;
  private final DateTime createdTime;
  private final DateTime scheduledTime;
  private final String message;

  @JsonCreator
  public IngestJobInfo(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("jobId") String jobId,
      @JsonProperty("jobState") JobState jobState,
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("scheduledTime") @Nullable DateTime scheduledTime,
      @JsonProperty("schema") @Nullable IngestSchema schema,
      @JsonProperty("message") @Nullable String message
  )
  {
    this.tableName = tableName;
    this.jobId = jobId;
    this.jobState = jobState;
    this.createdTime = createdTime;
    this.scheduledTime = scheduledTime;
    this.schema = schema;
    this.message = message;
  }

  static IngestJobInfo fromIngestJob(IngestJob job)
  {
    return new IngestJobInfo(
        job.getTableName(),
        job.getJobId(),
        job.getJobState(),
        job.getCreatedTime(),
        job.getScheduledTime(),
        job.getSchema(),
        job.getUserFacingMessage()
    );
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
  public String getMessage()
  {
    return message;
  }

  @JsonProperty("schema")
  @Nullable
  public IngestSchema getSchema()
  {
    return schema;
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
    IngestJobInfo that = (IngestJobInfo) o;
    return tableName.equals(that.tableName)
           && jobId.equals(that.jobId)
           && jobState == that.jobState
           && Objects.equals(schema, that.schema)
           && createdTime.equals(that.createdTime)
           && Objects.equals(scheduledTime, that.scheduledTime)
           && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tableName, jobId, jobState, schema, createdTime, scheduledTime, message);
  }

  @Override
  public String toString()
  {
    return "IngestJobInfo{" +
           "tableName='" + tableName + '\'' +
           ", jobId='" + jobId + '\'' +
           ", jobState=" + jobState +
           ", schema=" + schema +
           ", createdTime=" + createdTime +
           ", scheduledTime=" + scheduledTime +
           ", message='" + message + '\'' +
           '}';
  }
}
