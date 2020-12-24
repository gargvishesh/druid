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
import io.imply.druid.ingest.jobs.OverlordClient;
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import io.imply.druid.ingest.jobs.status.TaskBasedJobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;

import javax.annotation.Nullable;

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
    final IndexingServiceClient overlordClient = jobProcessingContext.getOverlordClient();
    final String previousTaskId = getTaskIdFromJobStatus(job);
    final String taskId;
    if (previousTaskId != null) {
      TaskStatusResponse previousAttempt = overlordClient.getTaskStatus(previousTaskId);
      // task status should not be null if the task actually exists (overlord can return an empty status response for
      // non-existent tasks)
      if (previousAttempt != null && previousAttempt.getStatus() != null) {
        LOG.warn(
            "Found job [%s], it already has a taskId in state [%s], but is in the [%s] state here. This should"
            + " not happen but could be a transient situation due to a system outage between scheduling"
            + " the task and updating to the running state. Allowing the job to transition state to %s.",
            job.getJobId(),
            previousAttempt.getStatus(),
            JobState.SCHEDULED,
            JobState.RUNNING
        );
        // let it fall through to handle by update status
        return true;
      }

      // lets try same task id again, it doesn't exist as far as overlord is concerned again
      taskId = previousTaskId;
    } else {
      taskId = generateTaskId(job.getJobId());
    }
    TaskBasedJobStatus initialStatus = new TaskBasedJobStatus(taskId, null, null);
    jobProcessingContext.getMetadataStore().setJobStatus(job.getJobId(), initialStatus);
    final Task theTask = createTask(
        taskId,
        job,
        // someday this might take a list of files instead of the jobId as the file...
        jobProcessingContext.getFileStore().makeInputSource(job.getJobId())
    );
    overlordClient.runTask(theTask.getId(), theTask);
    LOG.info("Successfully submitted task [%s] for job [%s]", taskId, job.getJobId());
    return true;
  }

  /**
   * Given an {@link IngestJob}, check the {@link TaskStatus} from the Druid Overlord.
   */
  @Override
  public JobUpdateStateResult updateJobStatus(JobProcessingContext jobProcessingContext, IngestJob job)
  {
    final OverlordClient overlordClient = jobProcessingContext.getOverlordClient();
    final String taskId = getTaskIdFromJobStatus(job);
    if (taskId != null) {
      TaskStatusResponse statusResponse = overlordClient.getTaskStatus(taskId);
      TaskReport taskReport = overlordClient.getTaskReport(taskId);
      final JobStatus jobStatus = new TaskBasedJobStatus(taskId, statusResponse.getStatus(), taskReport);
      final JobState jobState;
      if (statusResponse.getStatus().getStatusCode().isSuccess()) {
        jobState = JobState.COMPLETE;
      } else if (statusResponse.getStatus().getStatusCode().isFailure()) {
        jobState = JobState.FAILED;
      } else {
        LOG.info(
            "job [%s] task [%s] is in state [%s]",
            job.getJobId(),
            taskId,
            statusResponse.getStatus().getStatusCode()
        );
        jobState = JobState.RUNNING;
      }
      return new JobUpdateStateResult(jobState, jobStatus);
    } else {
      final String errorMessage = StringUtils.format("Cannot update status of job [%s], no taskId set", job.getJobId());
      LOG.error(errorMessage);
      return new JobUpdateStateResult(
          JobState.FAILED,
          new FailedJobStatus(errorMessage)
      );
    }
  }


  @VisibleForTesting
  public static Task createTask(
      String taskId,
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
    return new ParallelIndexSupervisorTask(taskId, null, null, spec, null);
  }

  @VisibleForTesting
  public static String generateTaskId(String jobId)
  {
    return StringUtils.format("ingest_append_job_%s_%s", jobId, DateTimes.nowUtc().getMillis());
  }

  @VisibleForTesting
  @Nullable
  public static String getTaskIdFromJobStatus(IngestJob job)
  {
    final JobStatus jobStatus = job.getJobStatus();
    if (jobStatus instanceof TaskBasedJobStatus) {
      return ((TaskBasedJobStatus) jobStatus).getTaskId();
    }
    return null;
  }
}
