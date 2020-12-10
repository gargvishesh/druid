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
import java.util.Set;

/**
 * Persistence layer for ingest service, for storage of ingest jobs, ingest schemas
 */
public interface IngestServiceMetadataStore
{

  // tables
  int insertTable(String name);

  boolean druidTableExists(String name);

  List<Table> getTables();

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

  /**
   * @param jobStatesToFilterOn If null then return all tables with associated job states even when the table does not
   *                            have jobs (in this case job states will be an empty list).
   *                            If the list is empty return tables with no jobs.
   *                            If the list is non-empty return tables only having states specified.
   * @return A list of tables with their associated job state count
   */
  Set<TableJobStateStats> getJobCountPerTablePerState(@Nullable Set<JobState> jobStatesToFilterOn);

  // schemas
  int createSchema(IngestSchema schema);
  IngestSchema getSchema(int schemaId);
  List<IngestSchema> getAllSchemas();
  int deleteSchema(int schemaId);

  // formats
  InputFormat getFormat(int formatId);
}
