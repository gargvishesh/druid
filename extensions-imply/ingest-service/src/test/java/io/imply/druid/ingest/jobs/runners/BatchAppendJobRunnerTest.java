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
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.PartitionScheme;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class BatchAppendJobRunnerTest
{
  @Test
  public void testCreateTaskWithNullPartitionSchemeUseDayGranularity()
  {
    final String tableName = "table";
    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"))
    );
    final InputFormat inputFormat = new JsonInputFormat(null, null, null);
    final IngestJob ingestJob = new IngestJob(tableName, "jobId", JobState.RUNNING);
    ingestJob.setCreatedTime(DateTimes.nowUtc());
    ingestJob.setSchema(
        new IngestSchema(timestampSpec, dimensionsSpec, null, inputFormat, "test")
    );
    final InputSource inputSource = new InlineInputSource("{ \"this\": \"is test\"}");
    final Task task = BatchAppendJobRunner.createTask(ingestJob, inputSource);

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
    final IngestJob ingestJob = new IngestJob(tableName, "jobId", JobState.RUNNING);
    ingestJob.setCreatedTime(DateTimes.nowUtc());
    ingestJob.setSchema(
        new IngestSchema(timestampSpec, dimensionsSpec, new PartitionScheme(Granularities.MONTH, null), inputFormat, "test")
    );
    final InputSource inputSource = new InlineInputSource("{ \"this\": \"is test\"}");
    final Task task = BatchAppendJobRunner.createTask(ingestJob, inputSource);

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

}
