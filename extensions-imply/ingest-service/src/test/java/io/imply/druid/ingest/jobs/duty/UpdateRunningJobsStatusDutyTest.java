/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.duty;

import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UpdateRunningJobsStatusDutyTest extends BaseJobsDutyTest
{
  UpdateRunningJobsStatusDuty jobUpdater;
  final JobRunner jobType = new BatchAppendJobRunner();

  @Before
  @Override
  public void setup()
  {
    super.setup();
    jobUpdater = new UpdateRunningJobsStatusDuty(jobProcessingContext);
  }

  @Test
  public void testDoesBasicallyNothingIfNoRunningJobs()
  {
    replayAll();
    jobUpdater.run();
  }

  @Test
  public void testFetchesTaskStatusForRunningJobsAndSetsTerminalStates()
  {
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);
    metadataStore.setJobState(job1, JobState.RUNNING);
    String job2 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job2, ingestSchema);
    metadataStore.setJobState(job2, JobState.RUNNING);
    String job3 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job3, ingestSchema);
    metadataStore.setJobState(job3, JobState.RUNNING);

    EasyMock.expect(indexingServiceClient.getTaskReport(job1)).andReturn(null);
    EasyMock.expect(indexingServiceClient.getTaskReport(job2)).andReturn(null);
    EasyMock.expect(indexingServiceClient.getTaskReport(job3)).andReturn(null);

    expectTaskRequest(job1, TaskState.SUCCESS);
    expectTaskRequest(job2, TaskState.FAILED);
    expectTaskRequest(job3, TaskState.RUNNING);
    replayAll();
    jobUpdater.run();

    Assert.assertEquals(JobState.COMPLETE, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(JobState.FAILED, metadataStore.getJob(job2).getJobState());
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job3).getJobState());
  }

  private void expectTaskRequest(String taskId, TaskState expectedState)
  {
    EasyMock.expect(indexingServiceClient.getTaskStatus(taskId))
            .andReturn(new TaskStatusResponse(taskId, makeTaskStatusPlus(taskId, expectedState)))
            .once();
  }

  TaskStatusPlus makeTaskStatusPlus(String jobId, TaskState taskState)
  {
    return new TaskStatusPlus(
        jobId,
        null,
        null,
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        taskState,
        null,
        null,
        1000L,
        TaskLocation.unknown(),
        TABLE,
        taskState.isFailure() ? "failed" : null
    );
  }
}
