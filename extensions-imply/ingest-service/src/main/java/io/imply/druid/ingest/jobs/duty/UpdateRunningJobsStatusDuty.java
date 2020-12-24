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
import io.imply.druid.ingest.jobs.JobUpdateStateResult;
import io.imply.druid.ingest.metadata.IngestJob;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.Objects;

/**
 * Scans for {@link IngestJob} which have state {@link JobState#RUNNING}, and try to start them using
 * {@link JobRunner#updateJobStatus}. The jobs themselves should transition
 * to the correct {@link JobState} if necessary.
 */
public class UpdateRunningJobsStatusDuty implements JobProcessorDuty
{
  private static final Logger LOG = new Logger(UpdateRunningJobsStatusDuty.class);

  private final JobProcessingContext jobProcessingContext;

  public UpdateRunningJobsStatusDuty(JobProcessingContext jobProcessingContext)
  {
    this.jobProcessingContext = jobProcessingContext;
  }

  @Override
  public void run()
  {
    List<IngestJob> jobs = jobProcessingContext.getMetadataStore().getJobs(JobState.RUNNING);
    if (jobs.isEmpty()) {
      LOG.debug("No running jobs");
      return;
    }

    LOG.info("Checking for status update of [%s] running jobs", jobs.size());
    try {
      for (IngestJob job : jobs) {
        JobUpdateStateResult result = job.getJobRunner().updateJobStatus(jobProcessingContext, job);
        if (result.getJobState() != job.getJobState()) {
          LOG.info(
              "Updating state to [%s] and status to [%s] for job [%s]",
              result.getJobState(),
              result.getJobStatus(),
              job.getJobId()
          );
          jobProcessingContext.getMetadataStore()
                              .setJobStateAndStatus(job.getJobId(), result.getJobState(), result.getJobStatus());
        } else if (!Objects.equals(job.getJobStatus(), result.getJobStatus())) {
          LOG.info("Updating status to [%s] for job [%s]", result.getJobStatus(), job.getJobId());
          jobProcessingContext.getMetadataStore()
                              .setJobStatus(job.getJobId(), result.getJobStatus());
        }
      }
    }
    catch (Exception ex) {
      LOG.warn(ex, "Exception while updating job status, will retry on next scheduled run.");
    }
  }
}
