/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.runners;

import com.google.common.annotations.VisibleForTesting;
import io.imply.druid.ingest.jobs.JobProcessingContext;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.JobStatus;
import io.imply.druid.ingest.jobs.JobUpdateStateResult;
import io.imply.druid.ingest.jobs.status.TaskBasedJobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;

/**
 * {@link JobRunner} which can submit batch 'append' {@link ParallelIndexSupervisorTask} to a Druid Overlord
 */
public class BatchAppendJobRunner implements JobRunner
{
  private static final Logger LOG = new Logger(BatchAppendJobRunner.class);

  /**
   * Given an {@link IngestJob}, convert it into a {@link ParallelIndexSupervisorTask} and submit it to the Druid
   * Overlord.
   */
  @Override
  public boolean startJob(JobProcessingContext jobProcessingContext, IngestJob job)
  {
    final IndexingServiceClient overlordClient = jobProcessingContext.getIndexingClient();
    final Task theTask = createTask(
        job,
        // someday this might take a list of files instead of the jobId as the file...
        jobProcessingContext.getFileStore().makeInputSource(job.getJobId())
    );
    String taskId = overlordClient.runTask(theTask.getId(), theTask);
    LOG.info("Successfully submitted task [%s] for job [%s]", taskId, job.getJobId());
    return true;
  }

  /**
   * Given an {@link IngestJob}, check the {@link TaskStatus} from the Druid Overlord.
   * @return
   */
  @Override
  public JobUpdateStateResult updateJobStatus(JobProcessingContext jobProcessingContext, IngestJob job)
  {
    final IndexingServiceClient overlordClient = jobProcessingContext.getIndexingClient();
    TaskStatusResponse statusResponse = overlordClient.getTaskStatus(job.getJobId());
    final JobState jobState;
    final JobStatus jobStatus = new TaskBasedJobStatus(statusResponse.getStatus());
    if (statusResponse.getStatus().getStatusCode().isSuccess()) {
      jobState = JobState.COMPLETE;
    } else if (statusResponse.getStatus().getStatusCode().isFailure()) {
      jobState = JobState.FAILED;
    } else {
      LOG.info("job [%s] is in state", statusResponse.getStatus().getStatusCode());
      jobState = JobState.RUNNING;
    }
    return new JobUpdateStateResult(jobState, jobStatus);
  }

  @VisibleForTesting
  public static Task createTask(
      IngestJob job,
      InputSource inputSource
  )
  {
    ParallelIndexIngestionSpec spec = new ParallelIndexIngestionSpec(
        new DataSchema(
            job.getTableName(),
            job.getSchema().getTimestampSpec(),
            job.getSchema().getDimensionsSpec(),
            null,
            new UniformGranularitySpec(job.getSchema().getPartitionScheme().getSegmentGranularity(), null, null),
            null
        ),
        new ParallelIndexIOConfig(null, inputSource, job.getSchema().getInputFormat(), true),
        // todo: probably want some external gizmo to compute this, maybe based on cluster state?
        ParallelIndexTuningConfig.defaultConfig()
    );
    return new ParallelIndexSupervisorTask(job.getJobId(), null, null, spec, null);
  }
}
