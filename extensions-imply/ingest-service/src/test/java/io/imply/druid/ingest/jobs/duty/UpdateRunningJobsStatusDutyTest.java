/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.duty;

import com.google.common.collect.ImmutableList;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.JobStatus;
import io.imply.druid.ingest.jobs.JobUpdateStateResult;
import org.easymock.EasyMock;
import org.junit.Test;

public class UpdateRunningJobsStatusDutyTest extends BaseJobsDutyTest
{
  @Test
  public void testUpdateDoesNothingOnNoPhaseChange()
  {
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();
    EasyMock.expect(job.getJobState()).andReturn(JobState.RUNNING).anyTimes();

    JobStatus mockStatus = EasyMock.createMock(JobStatus.class);
    EasyMock.expect(job.getJobStatus()).andReturn(mockStatus).once();

    EasyMock.expect(metadataStore.getJobs(JobState.RUNNING)).andReturn(ImmutableList.of(job)).once();
    EasyMock.expect(runner.updateJobStatus(jobProcessingContext, job))
            .andReturn(new JobUpdateStateResult(JobState.RUNNING, mockStatus))
            .once();

    replayAll();

    UpdateRunningJobsStatusDuty duty = new UpdateRunningJobsStatusDuty(jobProcessingContext);
    duty.run();
  }

  @Test
  public void testUpdatesStatusOnStatusChange()
  {
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();
    EasyMock.expect(job.getJobState()).andReturn(JobState.RUNNING).anyTimes();

    JobStatus mockStatus = EasyMock.createMock(JobStatus.class);
    JobStatus differentStatus = EasyMock.createMock(JobStatus.class);
    EasyMock.expect(job.getJobStatus()).andReturn(mockStatus).once();

    EasyMock.expect(metadataStore.getJobs(JobState.RUNNING)).andReturn(ImmutableList.of(job)).once();
    EasyMock.expect(runner.updateJobStatus(jobProcessingContext, job))
            .andReturn(new JobUpdateStateResult(JobState.RUNNING, differentStatus))
            .once();
    metadataStore.setJobStatus(JOB_ID, differentStatus);
    EasyMock.expectLastCall();

    replayAll();

    UpdateRunningJobsStatusDuty duty = new UpdateRunningJobsStatusDuty(jobProcessingContext);
    duty.run();
  }

  @Test
  public void testUpdatesStateAndStatusOnStateAndStatusChange()
  {
    JobStatus mockStatus = EasyMock.createMock(JobStatus.class);
    JobStatus differentStatus = EasyMock.createMock(JobStatus.class);
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();
    EasyMock.expect(job.getJobState()).andReturn(JobState.RUNNING).anyTimes();
    EasyMock.expect(job.getJobStatus()).andReturn(mockStatus).anyTimes();

    EasyMock.expect(metadataStore.getJobs(JobState.RUNNING)).andReturn(ImmutableList.of(job)).once();
    EasyMock.expect(runner.updateJobStatus(jobProcessingContext, job))
            .andReturn(new JobUpdateStateResult(JobState.COMPLETE, differentStatus))
            .once();
    metadataStore.setJobStateAndStatus(JOB_ID, JobState.COMPLETE, differentStatus);
    EasyMock.expectLastCall();

    replayAll();

    UpdateRunningJobsStatusDuty duty = new UpdateRunningJobsStatusDuty(jobProcessingContext);
    duty.run();
  }

  @Test
  public void testUpdatesStateAndStatusStatusOnStateChange()
  {
    JobStatus mockStatus = EasyMock.createMock(JobStatus.class);
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();
    EasyMock.expect(job.getJobState()).andReturn(JobState.RUNNING).anyTimes();
    EasyMock.expect(job.getJobStatus()).andReturn(mockStatus).anyTimes();

    EasyMock.expect(metadataStore.getJobs(JobState.RUNNING)).andReturn(ImmutableList.of(job)).once();
    EasyMock.expect(runner.updateJobStatus(jobProcessingContext, job))
            .andReturn(new JobUpdateStateResult(JobState.COMPLETE, mockStatus))
            .once();
    metadataStore.setJobStateAndStatus(JOB_ID, JobState.COMPLETE, mockStatus);
    EasyMock.expectLastCall();

    replayAll();

    UpdateRunningJobsStatusDuty duty = new UpdateRunningJobsStatusDuty(jobProcessingContext);
    duty.run();
  }
}
