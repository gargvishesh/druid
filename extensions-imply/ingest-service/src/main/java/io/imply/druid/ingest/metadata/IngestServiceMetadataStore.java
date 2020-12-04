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
import org.apache.druid.data.input.InputFormat;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Persistence layer for ingest service, for storage of ingest jobs, ingest schemas
 */
public interface IngestServiceMetadataStore
{

  // tables
  int insertTable(String name);
  boolean druidTableExists(String name);
  List<String> getAllTableNames();

  // jobs
  String stageJob(String tableName, JobRunner jobType);
  int scheduleJob(String jobId, IngestSchema schema);
  void setJobStatus(String jobId, JobStatus jobStatus);
  void setJobState(String jobId, JobState jobState);
  void setJobStateAndStatus(String jobId, @Nullable JobStatus status, @Nullable JobState jobState);
  void setJobCancelled(String jobId, JobStatus status);
  int jobRetry(String jobId);

  List<IngestJob> getJobs(@Nullable JobState jobState);
  @Nullable
  IngestJob getJob(String jobId);

  // schemas
  int createSchema(IngestSchema schema);
  IngestSchema getSchema(int schemaId);
  List<IngestSchema> getAllSchemas();
  int deleteSchema(int schemaId);

  // formats
  InputFormat getFormat(int formatId);
}
