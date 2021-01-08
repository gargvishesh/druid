/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.JobStatus;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunnerTest;
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import io.imply.druid.ingest.jobs.status.TaskBasedJobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.JobScheduleException;
import io.imply.druid.ingest.metadata.PartitionScheme;
import io.imply.druid.ingest.metadata.Table;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IngestServiceSqlMetadataStoreTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final String TABLE = "some_table";
  private static final String OTHER_TABLE = "other_table";
  private static final String ANOTHER_TABLE = "another_table";
  private static final IngestSchema TEST_SCHEMA = new IngestSchema(
      new TimestampSpec("time", "iso", null),
      new DimensionsSpec(
          ImmutableList.of(
              StringDimensionSchema.create("x"),
              StringDimensionSchema.create("y")
          )
      ),
      new PartitionScheme(Granularities.DAY, null),
      new JsonInputFormat(null, null, null),
      "test schema"
  );
  private final IngestServiceSqlMetatadataConfig config = IngestServiceSqlMetatadataConfig.DEFAULT_CONFIG;

  private IngestServiceSqlMetadataStore metadataStore;
  private SQLMetadataConnector connector;
  private final JobRunner jobType = new BatchAppendJobRunner();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Before
  public void setup()
  {
    // using derby because mocking was too complex
    connector = derbyConnectorRule.getConnector();
    metadataStore = new IngestServiceSqlMetadataStore(() -> config, connector, MAPPER);
  }

  @After
  public void teardown()
  {

  }

  @Test
  public void testCreateTablesTable()
  {
    Assert.assertTrue(connector.retryWithHandle((handle) -> connector.tableExists(handle, config.getTablesTable())));
  }

  @Test
  public void testCreateJobsTable()
  {
    Assert.assertTrue(connector.retryWithHandle((handle) -> connector.tableExists(handle, config.getJobsTable())));
  }

  @Test
  public void testCreateSchemasTable()
  {
    Assert.assertTrue(connector.retryWithHandle((handle) -> connector.tableExists(handle, config.getSchemasTable())));
  }

  @Test
  public void testTablesInsert()
  {
    List<Table> tables = metadataStore.getTables();
    Assert.assertEquals(0, tables.size());
    int updateCount = metadataStore.insertTable(TABLE);
    tables = metadataStore.getTables();
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE, tables.get(0).getName());
    Assert.assertEquals(1, updateCount);
  }

  @Test(expected = Exception.class)
  public void testTablesInsertDup()
  {
    int updateCount = metadataStore.insertTable(TABLE);
    metadataStore.insertTable(TABLE);
  }

  @Test
  public void testTablesExists()
  {
    Assert.assertFalse(metadataStore.druidTableExists(TABLE));
    metadataStore.insertTable(TABLE);
    Assert.assertTrue(metadataStore.druidTableExists(TABLE));
  }


  @Test
  public void testTableJobSummaryNoJobs()
  {
    metadataStore.insertTable(TABLE);
    metadataStore.insertTable(TABLE + "_1");

    Assert.assertEquals(0, metadataStore.getJobs(null).size());

    // expect 2 tables
    List<Table> tables = metadataStore.getTables();
    Assert.assertEquals(2, tables.size());

    // expect no running jobs,
    Map<Table, Object2IntMap<JobState>> summaries =
        metadataStore.getTableJobSummary(Collections.singleton(JobState.RUNNING));
    Assert.assertEquals(0, summaries.size());

    // expect two table summaries, null states
    summaries = metadataStore.getTableJobSummary(null);
    Assert.assertEquals(2, summaries.size());
    for (Table t : tables) {
      Assert.assertTrue(summaries.containsKey(t));
      Assert.assertNull(summaries.get(t));
    }
  }

  @Test
  public void testTableJobSummaryFilter()
  {
    metadataStore.insertTable(TABLE);
    metadataStore.insertTable(OTHER_TABLE);
    metadataStore.insertTable(ANOTHER_TABLE);

    List<Table> tables = metadataStore.getTables();
    Map<String, Table> tableMap = tables.stream().collect(Collectors.toMap(Table::getName, Function.identity()));

    // TABLE has scheduled, running, and complete jobs
    stageSomeJobsAndSetStates(TABLE, JobState.SCHEDULED, 3);
    stageSomeJobsAndSetStates(TABLE, JobState.RUNNING, 5);
    stageSomeJobsAndSetStates(TABLE, JobState.COMPLETE, 15);

    // OTHER_TABLE has scheduled and complete jobs
    stageSomeJobsAndSetStates(OTHER_TABLE, JobState.SCHEDULED, 1);
    stageSomeJobsAndSetStates(OTHER_TABLE, JobState.COMPLETE, 3);

    // ANOTHER_TABLE has no jobs

    // no job state filter, expect all tables, expect 2 state count maps, 1 null valued state count map since no jobs
    // on that table
    Map<Table, Object2IntMap<JobState>> summaries = metadataStore.getTableJobSummary(null);
    Assert.assertEquals(3, summaries.get(tableMap.get(TABLE)).keySet().size());
    Assert.assertEquals(5, summaries.get(tableMap.get(TABLE)).getInt(JobState.RUNNING));
    Assert.assertEquals(3, summaries.get(tableMap.get(TABLE)).getInt(JobState.SCHEDULED));
    Assert.assertEquals(15, summaries.get(tableMap.get(TABLE)).getInt(JobState.COMPLETE));

    Assert.assertEquals(2, summaries.get(tableMap.get(OTHER_TABLE)).keySet().size());
    Assert.assertEquals(1, summaries.get(tableMap.get(OTHER_TABLE)).getInt(JobState.SCHEDULED));
    Assert.assertEquals(3, summaries.get(tableMap.get(OTHER_TABLE)).getInt(JobState.COMPLETE));

    // this one has no jobs, expect empty map
    Assert.assertNull(summaries.get(tableMap.get(ANOTHER_TABLE)));

    // filter by running, expect all tables, expect 1 state count map, 2 null valued state count maps since no running
    // jobs on those tables
    summaries = metadataStore.getTableJobSummary(ImmutableSet.of(JobState.RUNNING));
    Assert.assertEquals(5, summaries.get(tableMap.get(TABLE)).getInt(JobState.RUNNING));
    Assert.assertFalse(summaries.get(tableMap.get(TABLE)).containsKey(JobState.SCHEDULED));
    Assert.assertFalse(summaries.get(tableMap.get(TABLE)).containsKey(JobState.COMPLETE));

    // this one has no running jobs, expect empty map
    Assert.assertNull(summaries.get(tableMap.get(OTHER_TABLE)));

    // this one has no jobs, expect empty map
    Assert.assertNull(summaries.get(tableMap.get(ANOTHER_TABLE)));

    // filter by scheduled, expect all tables, expect 2 state count maps, 1 null valued state count map since no jobs
    // on that table
    summaries = metadataStore.getTableJobSummary(ImmutableSet.of(JobState.SCHEDULED));
    Assert.assertEquals(3, summaries.get(tableMap.get(TABLE)).getInt(JobState.SCHEDULED));
    Assert.assertFalse(summaries.get(tableMap.get(TABLE)).containsKey(JobState.RUNNING));
    Assert.assertFalse(summaries.get(tableMap.get(TABLE)).containsKey(JobState.COMPLETE));

    Assert.assertEquals(1, summaries.get(tableMap.get(OTHER_TABLE)).getInt(JobState.SCHEDULED));
    Assert.assertFalse(summaries.get(tableMap.get(OTHER_TABLE)).containsKey(JobState.COMPLETE));

    // this one has no jobs, expect empty map
    Assert.assertNull(summaries.get(tableMap.get(ANOTHER_TABLE)));
  }

  private List<IngestJob> stageSomeJobsAndSetStates(String table, JobState jobState, int count)
  {
    List<IngestJob> jobs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String jobId = metadataStore.stageJob(table, jobType);
      metadataStore.scheduleJob(jobId, TEST_SCHEMA);
      String taskId = BatchAppendJobRunner.generateTaskId(jobId);
      JobStatus expectedStatus = new TaskBasedJobStatus(taskId, null, null);
      metadataStore.setJobStateAndStatus(jobId, jobState, expectedStatus);
      IngestJob job = metadataStore.getJob(jobId);
      Assert.assertNotNull(job);
      Assert.assertEquals(jobState, job.getJobState());
      Assert.assertEquals(expectedStatus, job.getJobStatus());
      jobs.add(job);
    }
    return jobs;
  }

  @Test
  public void testStageJob()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(jobId, job.getJobId());
    Assert.assertEquals(TABLE, job.getTableName());
    Assert.assertEquals(JobState.STAGED, job.getJobState());
    Assert.assertNotNull(job.getCreatedTime());

    Assert.assertNull(job.getScheduledTime());
    Assert.assertNull(job.getSchema());
    Assert.assertNull(job.getJobStatus());
  }

  @Test
  public void testScheduleJobShouldSucceedWhenJobExists()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    int updated = metadataStore.scheduleJob(jobId, TEST_SCHEMA);

    Assert.assertEquals(updated, 1);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.SCHEDULED, job.getJobState());
    Assert.assertNotNull(job.getScheduledTime());
    Assert.assertEquals(TEST_SCHEMA, job.getSchema());
  }

  @Test
  public void testRetryScheduleJobShouldFailWhenJobInRunningState()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    expectedException.expect(JobScheduleException.class);
    expectedException.expectMessage(
        StringUtils.format("Cannot schedule job [%s] because it is in [%s] state", jobId, JobState.RUNNING)
    );
    metadataStore.setJobState(jobId, JobState.RUNNING);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
  }

  @Test
  public void testRetryScheduleJobShouldFailWhenJobInCompletedState()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    expectedException.expect(JobScheduleException.class);
    expectedException.expectMessage(
        StringUtils.format("Cannot schedule job [%s] because it is in [%s] state", jobId, JobState.COMPLETE)
    );
    metadataStore.setJobState(jobId, JobState.COMPLETE);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
  }

  @Test
  public void testRetryScheduleJobShouldFailWhenJobInScheduledState()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    expectedException.expect(JobScheduleException.class);
    expectedException.expectMessage(
        StringUtils.format("Cannot schedule job [%s] because it is in [%s] state", jobId, JobState.SCHEDULED)
    );
    metadataStore.setJobState(jobId, JobState.SCHEDULED);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
  }

  @Test
  public void testRetryScheduleJobShouldSucceedWhenJobFailed()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.setJobState(jobId, JobState.FAILED);
    int updated = metadataStore.scheduleJob(jobId, TEST_SCHEMA);

    Assert.assertEquals(updated, 1);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.SCHEDULED, job.getJobState());
    Assert.assertNotNull(job.getScheduledTime());
    Assert.assertEquals(TEST_SCHEMA, job.getSchema());
  }

  @Test
  public void testRetryScheduleJobShouldSucceedWhenJobCancelled()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.setJobState(jobId, JobState.CANCELLED);
    int updated = metadataStore.scheduleJob(jobId, TEST_SCHEMA);

    Assert.assertEquals(updated, 1);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.SCHEDULED, job.getJobState());
    Assert.assertNotNull(job.getScheduledTime());
    Assert.assertEquals(TEST_SCHEMA, job.getSchema());
  }

  @Test
  public void testScheduleJobShouldFailWhenJobDoesNotExist()
  {
    String jobId = "i_do_not_exist";
    int updated = metadataStore.scheduleJob(jobId, TEST_SCHEMA);
    Assert.assertEquals(updated, 0);
  }

  @Test
  public void testSetJobRunning()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
    metadataStore.setJobState(jobId, JobState.RUNNING);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.RUNNING, job.getJobState());
  }


  @Test
  public void testSetJobComplete()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
    String taskId = BatchAppendJobRunner.generateTaskId(jobId);
    JobStatus expectedStatus = new TaskBasedJobStatus(
        taskId,
        BatchAppendJobRunnerTest.makeTaskStatusPlus(taskId, TaskState.SUCCESS),
        null
    );
    metadataStore.setJobStateAndStatus(jobId, JobState.COMPLETE, expectedStatus);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.COMPLETE, job.getJobState());
    Assert.assertEquals(expectedStatus, job.getJobStatus());
  }

  @Test
  public void testSetJobFailed()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
    metadataStore.setJobState(jobId, JobState.RUNNING);
    JobStatus expectedStatus = new FailedJobStatus("fail");
    metadataStore.setJobStateAndStatus(jobId, JobState.FAILED, expectedStatus);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.FAILED, job.getJobState());
    Assert.assertEquals(expectedStatus, job.getJobStatus());
  }

  @Test
  public void testSetJobCancelled()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
    metadataStore.setJobState(jobId, JobState.RUNNING);
    JobStatus expectedStatus = new FailedJobStatus("cancel");
    metadataStore.setJobCancelled(jobId, expectedStatus);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.CANCELLED, job.getJobState());
    Assert.assertEquals(expectedStatus, job.getJobStatus());
  }

  @Test
  public void testListScheduledJobs() throws InterruptedException
  {

    String job1 = metadataStore.stageJob(TABLE, jobType);
    String job2 = metadataStore.stageJob(TABLE, jobType);
    String job3 = metadataStore.stageJob(TABLE, jobType);
    String job4 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, TEST_SCHEMA);
    Thread.sleep(500);
    metadataStore.scheduleJob(job3, TEST_SCHEMA);
    Thread.sleep(500);
    metadataStore.scheduleJob(job2, TEST_SCHEMA);

    List<IngestJob> scheduledJobs = metadataStore.getJobs(JobState.SCHEDULED);
    Assert.assertEquals(3, scheduledJobs.size());
    // scheduled jobs should be ordered by schedule time
    Assert.assertEquals(job1, scheduledJobs.get(0).getJobId());
    Assert.assertEquals(job3, scheduledJobs.get(1).getJobId());
    Assert.assertEquals(job2, scheduledJobs.get(2).getJobId());
  }

  @Test
  public void testGetAllJobs()
  {
    String job1 = metadataStore.stageJob(TABLE, jobType);
    String job2 = metadataStore.stageJob(TABLE, jobType);
    String job3 = metadataStore.stageJob(TABLE, jobType);
    String job4 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, TEST_SCHEMA);
    metadataStore.scheduleJob(job2, TEST_SCHEMA);
    metadataStore.scheduleJob(job3, TEST_SCHEMA);
    Set<String> expectedScheduled = ImmutableSet.of(job1, job2, job3, job4);

    List<IngestJob> scheduledJobs = metadataStore.getJobs(null);
    Assert.assertEquals(4, scheduledJobs.size());
    Assert.assertTrue(scheduledJobs.stream().allMatch(job -> expectedScheduled.contains(job.getJobId())));
  }

  @Test
  public void testTablesGetAll()
  {
    Set<String> expected = ImmutableSet.of("foo", "bar", "another", "one-more");

    expected.forEach(table -> metadataStore.insertTable(table));

    List<Table> tables = metadataStore.getTables();
    Assert.assertEquals(4, tables.size());
    Assert.assertTrue(expected.containsAll(tables.stream().map(t -> t.getName()).collect(Collectors.toList())));
  }

  @Test
  public void testTablesGetAllEmpty()
  {
    List<Table> tables = metadataStore.getTables();
    Assert.assertEquals(0, tables.size());
  }


  @Test
  public void testIncrementRetry()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(jobId, TEST_SCHEMA);
    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(0, job.getRetryCount());
    metadataStore.jobRetry(jobId);
    job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(1, job.getRetryCount());
    metadataStore.jobRetry(jobId);
    metadataStore.jobRetry(jobId);
    job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(3, job.getRetryCount());
  }

  @Test
  public void testCreateAndGetSchemas()
  {
    IngestSchema someSchema2 = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("q"),
                StringDimensionSchema.create("z")
            )
        ),
        new PartitionScheme(Granularities.DAY, null),
        new JsonInputFormat(null, null, null),
        "test schema2"
    );

    int schemaId = metadataStore.createSchema(TEST_SCHEMA);
    Assert.assertEquals(schemaId, 1);
    Assert.assertTrue(metadataStore.schemaExists(schemaId));
    IngestSchema schemaFromMetadata = metadataStore.getSchema(1);
    Assert.assertEquals(TEST_SCHEMA, schemaFromMetadata);

    schemaId = metadataStore.createSchema(someSchema2);
    Assert.assertEquals(schemaId, 2);
    schemaFromMetadata = metadataStore.getSchema(2);
    Assert.assertEquals(someSchema2, schemaFromMetadata);

    List<IngestSchema> ingestSchemas = metadataStore.getAllSchemas();
    Assert.assertEquals(ImmutableList.of(TEST_SCHEMA, someSchema2), ingestSchemas);
  }

  @Test
  public void testJobWithExternalSchema()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    int schemaId = metadataStore.createSchema(TEST_SCHEMA);
    metadataStore.scheduleJob(jobId, schemaId);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(TEST_SCHEMA, job.getSchema());
  }

  @Test
  public void testDeleteSchema()
  {
    int schemaId = metadataStore.createSchema(TEST_SCHEMA);
    Assert.assertEquals(schemaId, 1);
    IngestSchema schemaFromMetadata = metadataStore.getSchema(1);
    Assert.assertEquals(TEST_SCHEMA, schemaFromMetadata);

    int numDeleted = metadataStore.deleteSchema(1);
    Assert.assertEquals(1, numDeleted);
    schemaFromMetadata = metadataStore.getSchema(1);
    Assert.assertNull(schemaFromMetadata);
    numDeleted = metadataStore.deleteSchema(1);
    Assert.assertEquals(0, numDeleted);

    numDeleted = metadataStore.deleteSchema(999);
    Assert.assertEquals(0, numDeleted);
  }
}
