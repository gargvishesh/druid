/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.runners;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.duty.StartScheduledJobsDuty;
import io.imply.druid.ingest.jobs.duty.UpdateRunningJobsStatusDuty;
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import io.imply.druid.ingest.jobs.status.TaskBasedJobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.PartitionScheme;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class BatchAppendJobRunnerTest extends BaseJobRunnerTest
{
  JobRunner jobType = new BatchAppendJobRunner();
  StartScheduledJobsDuty jobScheduler;
  UpdateRunningJobsStatusDuty jobUpdater;

  @Override
  @Before
  public void setup()
  {
    super.setup();
    jobScheduler = new StartScheduledJobsDuty(jobProcessingContext);
    jobUpdater = new UpdateRunningJobsStatusDuty(jobProcessingContext);
  }

  @Test
  public void testCreateTaskWithNullPartitionSchemeUseDayGranularity()
  {
    final String tableName = "table";
    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"))
    );
    final InputFormat inputFormat = new JsonInputFormat(null, null, null);
    final String taskId = BatchAppendJobRunner.generateTaskId("jobId");
    final IngestJob ingestJob = new IngestJob(
        tableName,
        "jobId",
        JobState.RUNNING,
        new TaskBasedJobStatus(taskId, null, null),
        new BatchAppendJobRunner(),
        new IngestSchema(timestampSpec, dimensionsSpec, null, inputFormat, "test"),
        DateTimes.nowUtc(),
        null,
        0
    );
    final InputSource inputSource = new InlineInputSource("{ \"this\": \"is test\"}");
    final Task task = BatchAppendJobRunner.createTask(
        BatchAppendJobRunner.getTaskIdFromJobStatus(ingestJob),
        ingestJob,
        inputSource
    );

    Assert.assertSame(ParallelIndexSupervisorTask.class, task.getClass());
    final ParallelIndexSupervisorTask supervisorTask = (ParallelIndexSupervisorTask) task;
    Assert.assertEquals(tableName, supervisorTask.getDataSource());
    final DataSchema dataSchema = supervisorTask.getIngestionSchema().getDataSchema();
    Assert.assertEquals(timestampSpec, dataSchema.getTimestampSpec());
    Assert.assertEquals(
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2")),
            ImmutableList.of(timestampSpec.getTimestampColumn()),
            null
        ),
        dataSchema.getDimensionsSpec()
    );
    Assert.assertEquals(new UniformGranularitySpec(Granularities.DAY, null, null), dataSchema.getGranularitySpec());
    Assert.assertEquals(inputSource, supervisorTask.getIngestionSchema().getIOConfig().getInputSource());
    Assert.assertEquals(inputFormat, supervisorTask.getIngestionSchema().getIOConfig().getInputFormat());
  }

  @Test
  public void testCreateTaskWithCustomPartitionScheme()
  {
    final String tableName = "table";
    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"))
    );
    final InputFormat inputFormat = new JsonInputFormat(null, null, null);
    final String taskId = BatchAppendJobRunner.generateTaskId("jobId");
    final IngestJob ingestJob = new IngestJob(
        tableName,
        "jobId",
        JobState.RUNNING,
        new TaskBasedJobStatus(taskId, null, null),
        new BatchAppendJobRunner(),
        new IngestSchema(timestampSpec, dimensionsSpec, new PartitionScheme(Granularities.MONTH, null), inputFormat, "test"),
        DateTimes.nowUtc(),
        null,
        0
    );

    final InputSource inputSource = new InlineInputSource("{ \"this\": \"is test\"}");
    final Task task = BatchAppendJobRunner.createTask(taskId, ingestJob, inputSource);

    Assert.assertSame(ParallelIndexSupervisorTask.class, task.getClass());
    final ParallelIndexSupervisorTask supervisorTask = (ParallelIndexSupervisorTask) task;
    Assert.assertEquals(tableName, supervisorTask.getDataSource());
    final DataSchema dataSchema = supervisorTask.getIngestionSchema().getDataSchema();
    Assert.assertEquals(timestampSpec, dataSchema.getTimestampSpec());
    Assert.assertEquals(
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2")),
            ImmutableList.of(timestampSpec.getTimestampColumn()),
            null
        ),
        dataSchema.getDimensionsSpec()
    );
    Assert.assertEquals(new UniformGranularitySpec(Granularities.MONTH, null, null), dataSchema.getGranularitySpec());
    Assert.assertEquals(inputSource, supervisorTask.getIngestionSchema().getIOConfig().getInputSource());
    Assert.assertEquals(inputFormat, supervisorTask.getIngestionSchema().getIOConfig().getInputFormat());
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
    Assert.assertNotNull(((TaskBasedJobStatus) metadataStore.getJob(job1).getJobStatus()).getTaskId());
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job2).getJobState());
    Assert.assertNotNull(((TaskBasedJobStatus) metadataStore.getJob(job2).getJobStatus()).getTaskId());
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job3).getJobState());
    Assert.assertNotNull(((TaskBasedJobStatus) metadataStore.getJob(job3).getJobStatus()).getTaskId());
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
    EasyMock.expect(indexingServiceClient.getTaskStatus(EasyMock.anyString()))
            .andReturn(new TaskStatusResponse(job1, null)).once();
    replayAll();
    jobScheduler.run();
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job1).getJobState());
    Assert.assertNotNull(((TaskBasedJobStatus) metadataStore.getJob(job1).getJobStatus()).getTaskId());
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
    EasyMock.expect(indexingServiceClient.getTaskStatus(EasyMock.anyString()))
            .andReturn(null).times(3);
    replayAll();
    jobScheduler.run();
    jobScheduler.run();
    jobScheduler.run();
    jobScheduler.run();
    Assert.assertEquals(JobState.FAILED, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(4, metadataStore.getJob(job1).getRetryCount());
  }

  @Test
  public void testJobCanBeRescheduled()
  {
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);
    expectJobIdScheduled(metadataStore.getJob(job1));
    replayAll();
    jobScheduler.run();

    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job1).getJobState());
    final String taskId = ((TaskBasedJobStatus) metadataStore.getJob(job1).getJobStatus()).getTaskId();
    Assert.assertNotNull(taskId);
  }

  @Test
  public void testJobIsSetToRunningIfScheduledAndHasrunningTask()
  {
    final String existingTaskId = "someTaskId";
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);
    metadataStore.setJobStatus(job1, new TaskBasedJobStatus(existingTaskId, null, null));
    EasyMock.expect(indexingServiceClient.getTaskStatus(existingTaskId))
            .andReturn(new TaskStatusResponse(existingTaskId, makeTaskStatusPlus(existingTaskId, TaskState.RUNNING)))
            .once();
    replayAll();
    jobScheduler.run();

    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job1).getJobState());
    final String taskId = ((TaskBasedJobStatus) metadataStore.getJob(job1).getJobStatus()).getTaskId();
    Assert.assertNotNull(taskId);
  }

  @Test
  public void testJobIsScheduledUsingExistingTaskIdIfNoRunningTask()
  {
    final String existingTaskId = "someTaskId";
    String job1 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, ingestSchema);
    metadataStore.setJobStatus(job1, new TaskBasedJobStatus(existingTaskId, null, null));
    expectJobIdScheduled(metadataStore.getJob(job1), existingTaskId);
    EasyMock.expect(indexingServiceClient.getTaskStatus(existingTaskId)).andReturn(null).once();
    replayAll();
    jobScheduler.run();

    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job1).getJobState());
    final String taskId = ((TaskBasedJobStatus) metadataStore.getJob(job1).getJobStatus()).getTaskId();
    Assert.assertNotNull(taskId);
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
    String task1 = BatchAppendJobRunner.generateTaskId(job1);
    metadataStore.scheduleJob(job1, ingestSchema);
    metadataStore.setJobStateAndStatus(job1, JobState.RUNNING, new TaskBasedJobStatus(task1, null, null));
    String job2 = metadataStore.stageJob(TABLE, jobType);
    String task2 = BatchAppendJobRunner.generateTaskId(job2);
    metadataStore.scheduleJob(job2, ingestSchema);
    metadataStore.setJobStateAndStatus(job2, JobState.RUNNING, new TaskBasedJobStatus(task2, null, null));
    String job3 = metadataStore.stageJob(TABLE, jobType);
    String task3 = BatchAppendJobRunner.generateTaskId(job3);
    metadataStore.scheduleJob(job3, ingestSchema);
    metadataStore.setJobStateAndStatus(job3, JobState.RUNNING, new TaskBasedJobStatus(task3, null, null));


    expectTaskRequest(task1, TaskState.SUCCESS);
    expectTaskRequest(task2, TaskState.FAILED);
    expectTaskRequest(task3, TaskState.RUNNING);
    replayAll();
    jobUpdater.run();

    Assert.assertEquals(JobState.COMPLETE, metadataStore.getJob(job1).getJobState());
    Assert.assertEquals(JobState.FAILED, metadataStore.getJob(job2).getJobState());
    Assert.assertEquals(JobState.RUNNING, metadataStore.getJob(job3).getJobState());
  }

  @Test
  public void testUpdateWithoutTaskStatusIsFailure()
  {
    // this shouldn't be able to really happen, but add a test just in case
    String job = metadataStore.stageJob(TABLE, jobType);
    metadataStore.setJobStateAndStatus(job, JobState.RUNNING, new FailedJobStatus("how can this be?"));
    replayAll();
    jobUpdater.run();

    Assert.assertEquals(JobState.FAILED, metadataStore.getJob(job).getJobState());
    Assert.assertEquals(
        new FailedJobStatus(StringUtils.format("Cannot update status of job [%s], no taskId set", job)),
        metadataStore.getJob(job).getJobStatus()
    );
  }

  private void expectJobIdScheduled(IngestJob job)
  {
    Assert.assertNotNull(job);
    final String jobId = job.getJobId();

    final InputSource inputSource =
        new LocalInputSource(null, null, ImmutableSet.of(new File("/tmp/test/" + jobId)));

    EasyMock.expect(fileStore.makeInputSource(jobId))
            .andReturn(inputSource)
            .once();
    EasyMock.expect(indexingServiceClient.runTask(EasyMock.anyString(), EasyMock.anyObject(Task.class)))
            .andReturn("fakeTaskId").once();
  }

  private void expectJobIdScheduled(IngestJob job, String taskId)
  {
    Assert.assertNotNull(job);
    final String jobId = job.getJobId();

    final InputSource inputSource =
        new LocalInputSource(null, null, ImmutableSet.of(new File("/tmp/test/" + jobId)));

    EasyMock.expect(fileStore.makeInputSource(jobId))
            .andReturn(inputSource)
            .once();
    EasyMock.expect(indexingServiceClient.runTask(EasyMock.eq(taskId), EasyMock.anyObject(Task.class)))
            .andReturn(taskId).once();
  }

  private void expectJobIdServiceUnavailable(IngestJob job)
  {
    Assert.assertNotNull(job);
    final String jobId = job.getJobId();
    final String taskId = BatchAppendJobRunner.getTaskIdFromJobStatus(job);
    metadataStore.setJobStatus(job.getJobId(), new TaskBasedJobStatus(taskId, null, null));

    final InputSource inputSource =
        new LocalInputSource(null, null, ImmutableSet.of(new File("/tmp/test/" + jobId)));
    EasyMock.expect(fileStore.makeInputSource(jobId))
            .andReturn(inputSource)
            .once();
    EasyMock.expect(indexingServiceClient.runTask(EasyMock.anyString(), EasyMock.anyObject(Task.class)))
            .andThrow(new ISE("failed for some reason"))
            .once();
  }

  private void expectTaskRequest(String taskId, TaskState expectedState)
  {
    EasyMock.expect(indexingServiceClient.getTaskStatus(taskId))
            .andReturn(new TaskStatusResponse(taskId, makeTaskStatusPlus(taskId, expectedState)))
            .once();
    EasyMock.expect(indexingServiceClient.getTaskReport(taskId))
            .andReturn(null)
            .anyTimes();
  }

  public static TaskStatusPlus makeTaskStatusPlus(String taskId, TaskState taskState)
  {
    return new TaskStatusPlus(
        taskId,
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
