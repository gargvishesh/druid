/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.ingest.jobs.runners;

import com.google.common.collect.ImmutableList;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
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
  public void testCreateTask()
  {
    final String tableName = "table";
    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"))
    );
    final InputFormat inputFormat = new JsonInputFormat(null, null, null);
    final IngestJob ingestJob = new IngestJob(tableName, "jobId", JobState.RUNNING);
    ingestJob.setCreatedTime(DateTimes.nowUtc());
    ingestJob.setSchema(new IngestSchema(timestampSpec, dimensionsSpec, inputFormat, "test"));
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
