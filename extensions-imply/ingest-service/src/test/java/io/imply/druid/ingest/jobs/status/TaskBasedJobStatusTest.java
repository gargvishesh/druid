/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunnerTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

public class TaskBasedJobStatusTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 10, "processedWithError", 0, "unparseable", 0);

    TaskBasedJobStatus taskBasedJobStatus = makeJobStatus(determinePartitionsMap, "An error");

    String there = MAPPER.writeValueAsString(taskBasedJobStatus);
    TaskBasedJobStatus andBack = MAPPER.readValue(there, TaskBasedJobStatus.class);

    Assert.assertEquals(taskBasedJobStatus, andBack);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(TaskBasedJobStatus.class).usingGetClass().verify();
  }

  @Test
  public void testTaskBasedJobStatusWithError()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 10, "processedWithError", 0, "unparseable", 0);
    TaskBasedJobStatus taskBasedJobStatus = makeJobStatus(determinePartitionsMap, "An error");
    String message = taskBasedJobStatus.getMessage();
    Assert.assertTrue(message.startsWith("Error: An error"));
  }

  @Test
  public void testTaskBasedJobStatusWithNoError()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 10, "processedWithError", 0, "unparseable", 0);

    TaskBasedJobStatus taskBasedJobStatus = makeJobStatus(determinePartitionsMap, null);
    String message = taskBasedJobStatus.getMessage();
    Assert.assertFalse(message.startsWith("Error:"));
  }


  @Test
  public void testNullTaskReport()
  {
    TaskBasedJobStatus taskBasedJobStatus = makeJobStatus(null, null);
    String message = taskBasedJobStatus.getMessage();
    Assert.assertNull(message);
  }

  @Test
  public void testSchemaHint()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 0, "processedWithError", 0, "unparseable", 10);

    TaskBasedJobStatus taskBasedJobStatus = makeJobStatus(determinePartitionsMap, null);
    String message = taskBasedJobStatus.getMessage();
    Assert.assertTrue(message.contains("Hint: verify that the inputFormat"));
  }

  @Test
  public void testInputFileHint()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 0, "processedWithError", 0, "unparseable", 0);


    TaskBasedJobStatus taskBasedJobStatus = makeJobStatus(determinePartitionsMap, null);
    String message = taskBasedJobStatus.getMessage();
    Assert.assertTrue(message.contains("Hint: verify that the input file exists"));
  }

  private TaskBasedJobStatus makeJobStatus(
      @Nullable Map<String, Integer> determinePartitionsMap,
      @Nullable String error
  )
  {
    final String taskId = BatchAppendJobRunner.generateTaskId(UUIDUtils.generateUuid());
    final TaskReport taskReport;
    if (determinePartitionsMap != null) {
      final Map<String, Object> rowStats = ImmutableMap.of("determinePartitions", determinePartitionsMap);
      IngestionStatsAndErrorsTaskReportData payload =
          new IngestionStatsAndErrorsTaskReportData(IngestionState.COMPLETED, null, rowStats, error);

      taskReport = new IngestionStatsAndErrorsTaskReport(taskId, payload);
    } else {
      taskReport = null;
    }

    final TaskStatusPlus statusPlus = BatchAppendJobRunnerTest.makeTaskStatusPlus(
        taskId,
        error == null ? TaskState.SUCCESS : TaskState.FAILED
    );

    return new TaskBasedJobStatus("taskId", statusPlus, taskReport);
  }
}
