/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.duty;

import io.imply.druid.ingest.jobs.JobProcessingContext;
import io.imply.druid.ingest.jobs.JobProcessorDuty;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Scans for {@link IngestJob} which have state {@link JobState#SCHEDULED}, and try to start them using
 * {@link JobRunner#startJob}. If successful, the job will transition to
 * {@link JobState#RUNNING}.
 */
public class StartScheduledJobsDuty implements JobProcessorDuty
{
  private static final Logger LOG = new Logger(StartScheduledJobsDuty.class);

  private final JobProcessingContext jobProcessingContext;

  public StartScheduledJobsDuty(JobProcessingContext jobProcessingContext)
  {
    this.jobProcessingContext = jobProcessingContext;
  }

  @Override
  public void run()
  {
    List<IngestJob> jobs = jobProcessingContext.getMetadataStore().getJobs(JobState.SCHEDULED);
    if (jobs.size() > 0) {
      LOG.info("Starting [%s] scheduled jobs", jobs.size());
      PriorityQueue<IngestJob> jobsQueue = new PriorityQueue<>(
          Comparator.comparing(IngestJob::getScheduledTime)
      );
      for (IngestJob job : jobs) {
        jobsQueue.offer(job);
      }
      while (!jobsQueue.isEmpty()) {
        startJob(jobsQueue.poll());
      }
    } else {
      LOG.debug("No jobs to schedule.");
    }
  }

  void startJob(IngestJob job)
  {
    boolean success = false;
    try {
      LOG.info("Starting job [%s]", job.getJobId());
      success = job.getJobRunner().startJob(jobProcessingContext, job);
      if (success) {
        jobProcessingContext.getMetadataStore().setJobState(job.getJobId(), JobState.RUNNING);
      }
    }
    catch (Exception ex) {
      // todo: does this count for retries too?
      LOG.warn(
          ex,
          "Exception while submitting tasks to overlord for job [%s]. will retry on next scheduled run.",
          job.getJobId()
      );
    }
    finally {
      if (!success) {
        // todo: this should be configurable, also should we really do it? should it be time based in addition to count
        //       based?
        int numRetries = jobProcessingContext.getMetadataStore().jobRetry(job.getJobId());
        if (numRetries > 3) {
          LOG.warn(
              "Too many failures for job[%s], changing state to [%s]",
              job.getJobId(),
              JobState.FAILED
          );
          jobProcessingContext.getMetadataStore().setJobStateAndStatus(
              job.getJobId(),
              new FailedJobStatus("too many retries"),
              JobState.FAILED
          );
        }
      }
    }
  }
}
