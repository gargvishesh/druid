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
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import io.imply.druid.ingest.jobs.status.TaskBasedJobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class IngestServiceSqlMetadataStoreTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final String TABLE = "some_table";
  private final IngestServiceSqlMetatadataConfig config = IngestServiceSqlMetatadataConfig.DEFAULT_CONFIG;

  private IngestServiceSqlMetadataStore metadataStore;
  private SQLMetadataConnector connector;
  private final JobRunner jobType = new BatchAppendJobRunner();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Before
  public void setup()
  {
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
  public void testInsertTable()
  {
    List<String> tables = metadataStore.getAllTableNames();
    Assert.assertEquals(0, tables.size());
    int updateCount = metadataStore.insertTable(TABLE);
    tables = metadataStore.getAllTableNames();
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE, tables.get(0));
    Assert.assertEquals(1, updateCount);
  }

  @Test
  public void testTableExists()
  {
    Assert.assertFalse(metadataStore.druidTableExists("not-yet"));
    metadataStore.insertTable(TABLE);
    Assert.assertTrue(metadataStore.druidTableExists(TABLE));
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
    Assert.assertNull(job.getSchemaId());
    Assert.assertNull(job.getJobStatus());
  }

  @Test
  public void testScheduleJobShouldSucceedWhenJobExists()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );
    int updated = metadataStore.scheduleJob(jobId, someSchema);

    Assert.assertEquals(updated, 1);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.SCHEDULED, job.getJobState());
    Assert.assertNotNull(job.getScheduledTime());
    Assert.assertEquals(someSchema, job.getSchema());
  }

  @Test
  public void testScheduleJobShouldFailWhenJobDoesNotExist()
  {
    String jobId = "i_do_not_exist";
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    int updated = metadataStore.scheduleJob(jobId, someSchema);

    Assert.assertEquals(updated, 0);

  }

  @Test
  public void testSetJobRunning()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );
    metadataStore.scheduleJob(jobId, someSchema);
    metadataStore.setJobState(jobId, JobState.RUNNING);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.RUNNING, job.getJobState());
  }


  @Test
  public void testSetJobComplete()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );
    metadataStore.scheduleJob(jobId, someSchema);
    metadataStore.setJobState(jobId, JobState.RUNNING);
    JobStatus expectedStatus = new TaskBasedJobStatus(null);
    metadataStore.setJobStateAndStatus(jobId, expectedStatus, JobState.COMPLETE);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.COMPLETE, job.getJobState());
    Assert.assertEquals(expectedStatus, job.getJobStatus());
  }

  @Test
  public void testSetJobFailed()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );
    metadataStore.scheduleJob(jobId, someSchema);
    metadataStore.setJobState(jobId, JobState.RUNNING);
    JobStatus expectedStatus = new FailedJobStatus("fail");
    metadataStore.setJobStateAndStatus(jobId, expectedStatus, JobState.FAILED);

    IngestJob job = metadataStore.getJob(jobId);
    Assert.assertNotNull(job);
    Assert.assertEquals(JobState.FAILED, job.getJobState());
    Assert.assertEquals(expectedStatus, job.getJobStatus());
  }

  @Test
  public void testSetJobCancelled()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );
    metadataStore.scheduleJob(jobId, someSchema);
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
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    String job1 = metadataStore.stageJob(TABLE, jobType);
    String job2 = metadataStore.stageJob(TABLE, jobType);
    String job3 = metadataStore.stageJob(TABLE, jobType);
    String job4 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, someSchema);
    Thread.sleep(500);
    metadataStore.scheduleJob(job3, someSchema);
    Thread.sleep(500);
    metadataStore.scheduleJob(job2, someSchema);

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
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    String job1 = metadataStore.stageJob(TABLE, jobType);
    String job2 = metadataStore.stageJob(TABLE, jobType);
    String job3 = metadataStore.stageJob(TABLE, jobType);
    String job4 = metadataStore.stageJob(TABLE, jobType);
    metadataStore.scheduleJob(job1, someSchema);
    metadataStore.scheduleJob(job2, someSchema);
    metadataStore.scheduleJob(job3, someSchema);
    Set<String> expectedScheduled = ImmutableSet.of(job1, job2, job3, job4);

    List<IngestJob> scheduledJobs = metadataStore.getJobs(null);
    Assert.assertEquals(4, scheduledJobs.size());
    Assert.assertTrue(scheduledJobs.stream().allMatch(job -> expectedScheduled.contains(job.getJobId())));
  }

  @Test
  public void testIncrementRetry()
  {
    String jobId = metadataStore.stageJob(TABLE, jobType);
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );
    metadataStore.scheduleJob(jobId, someSchema);
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
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    IngestSchema someSchema2 = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("q"),
                StringDimensionSchema.create("z")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema2"
    );

    int schemaId = metadataStore.createSchema(someSchema);
    Assert.assertEquals(schemaId, 1);
    IngestSchema schemaFromMetadata = metadataStore.getSchema(1);
    Assert.assertEquals(someSchema, schemaFromMetadata);

    schemaId = metadataStore.createSchema(someSchema2);
    Assert.assertEquals(schemaId, 2);
    schemaFromMetadata = metadataStore.getSchema(2);
    Assert.assertEquals(someSchema2, schemaFromMetadata);

    List<IngestSchema> ingestSchemas = metadataStore.getAllSchemas();
    Assert.assertEquals(ImmutableList.of(someSchema, someSchema2), ingestSchemas);
  }

  @Test
  public void testDeleteSchema()
  {
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    int schemaId = metadataStore.createSchema(someSchema);
    Assert.assertEquals(schemaId, 1);
    IngestSchema schemaFromMetadata = metadataStore.getSchema(1);
    Assert.assertEquals(someSchema, schemaFromMetadata);

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
