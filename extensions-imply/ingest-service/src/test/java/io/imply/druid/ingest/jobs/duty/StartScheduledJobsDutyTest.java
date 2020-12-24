/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.duty;

import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.Test;

public class StartScheduledJobsDutyTest extends BaseJobsDutyTest
{
  @Test
  public void testStartJobSuccess()
  {
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();

    EasyMock.expect(runner.startJob(jobProcessingContext, job)).andReturn(true).once();
    metadataStore.setJobState(JOB_ID, JobState.RUNNING);
    EasyMock.expectLastCall();

    replayAll();

    StartScheduledJobsDuty duty = new StartScheduledJobsDuty(jobProcessingContext);
    duty.startJob(job);
  }

  @Test
  public void testStartJobFailureIncrementsRetry()
  {
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();

    EasyMock.expect(runner.startJob(jobProcessingContext, job)).andReturn(false).once();
    EasyMock.expect(metadataStore.jobRetry(JOB_ID)).andReturn(1).once();

    replayAll();

    StartScheduledJobsDuty duty = new StartScheduledJobsDuty(jobProcessingContext);
    duty.startJob(job);
  }

  @Test
  public void testStartJobTooManyFailureFails()
  {
    EasyMock.expect(job.getJobId()).andReturn(JOB_ID).anyTimes();
    EasyMock.expect(job.getJobRunner()).andReturn(runner).once();

    EasyMock.expect(runner.startJob(jobProcessingContext, job)).andReturn(false).once();
    final int numRetries = 1000;
    EasyMock.expect(metadataStore.jobRetry(JOB_ID)).andReturn(numRetries).once();
    metadataStore.setJobStateAndStatus(
        JOB_ID,
        JobState.FAILED,
        new FailedJobStatus(
            StringUtils.format("Unable to successfully submit task after [%s] attempts", numRetries)
        )
    );
    EasyMock.expectLastCall();

    replayAll();

    StartScheduledJobsDuty duty = new StartScheduledJobsDuty(jobProcessingContext);
    duty.startJob(job);
  }
}
