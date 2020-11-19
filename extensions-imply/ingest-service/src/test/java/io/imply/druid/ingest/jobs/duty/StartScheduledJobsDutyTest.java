/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.duty;

import com.google.common.collect.ImmutableSet;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.metadata.IngestJob;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class StartScheduledJobsDutyTest extends BaseJobsDutyTest
{
  StartScheduledJobsDuty jobScheduler;
  JobRunner jobType = new BatchAppendJobRunner();

  @Before
  @Override
  public void setup()
  {
    super.setup();
    jobScheduler = new StartScheduledJobsDuty(jobProcessingContext);
  }

  @Test
  public void testDoesBasicallyNothingIfNoJobsToSchedule()
  {
    replayAll();
    jobScheduler.run();
  }

  @Test
  public void testSubmitsJobsThatHaveBeenScheduledToOverlordInTheCorrectOrderAndSetsStatusToRunning()
  {
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);
    String job2 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job2, ingestSchema);
    String job3 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job3, ingestSchema);
    expectJobIdScheduled(metadataStore.getJob(job1));
    expectJobIdScheduled(metadataStore.getJob(job2));
    expectJobIdScheduled(metadataStore.getJob(job3));
    replayAll();
    jobScheduler.run();

    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job2).getJobState());
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job3).getJobState());
  }

  @Test
  public void testIncrementsRetryIfOverlordSubmitFailsAndTriesAgainLater()
  {
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);

    expectJobIdServiceUnavailable(metadataStore.getJob(job1));
    replayAll();
    jobScheduler.run();
    Assert.assertEquals(JobState.SCHEDULED, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(1, metadataStore.getJob(job1).getRetryCount());
    verifyAll();
    resetAll();

    expectJobIdScheduled(metadataStore.getJob(job1));
    replayAll();
    jobScheduler.run();
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(1, metadataStore.getJob(job1).getRetryCount());
  }


  @Test
  public void testIfTooManyRetriesFailsJob()
  {
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);

    expectJobIdServiceUnavailable(metadataStore.getJob(job1));
    expectJobIdServiceUnavailable(metadataStore.getJob(job1));
    expectJobIdServiceUnavailable(metadataStore.getJob(job1));
    expectJobIdServiceUnavailable(metadataStore.getJob(job1));
    replayAll();
    jobScheduler.run();
    jobScheduler.run();
    jobScheduler.run();
    jobScheduler.run();
    Assert.assertEquals(JobState.FAILED, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(4, metadataStore.getJob(job1).getRetryCount());
  }

  private void expectJobIdScheduled(IngestJob job)
  {
    Assert.assertNotNull(job);
    final String jobId = job.getJobId();
    final InputSource inputSource =
        new LocalInputSource(null, null, ImmutableSet.of(new File("/tmp/test/" + jobId)));
    final Task theTask = BatchAppendJobRunner.createTask(
        job,
        inputSource
    );
    EasyMock.expect(fileStore.makeInputSource(jobId))
            .andReturn(inputSource)
            .once();
    EasyMock.expect(indexingServiceClient.runTask(jobId, theTask)).andReturn(jobId).once();
  }

  private void expectJobIdServiceUnavailable(IngestJob job)
  {
    Assert.assertNotNull(job);
    final String jobId = job.getJobId();
    final InputSource inputSource =
        new LocalInputSource(null, null, ImmutableSet.of(new File("/tmp/test/" + jobId)));
    final Task theTask = BatchAppendJobRunner.createTask(
        job,
        inputSource
    );
    EasyMock.expect(fileStore.makeInputSource(jobId))
            .andReturn(inputSource)
            .once();
    EasyMock.expect(indexingServiceClient.runTask(jobId, theTask))
            .andThrow(new ISE("failed for some reason"))
            .once();
  }
}
