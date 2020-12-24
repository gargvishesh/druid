/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.status;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TaskBasedJobStatusTest
{

  @Test
  public void testTaskBasedJobStatusWithError()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 10, "processedWithError", 0, "unparseable", 0);
    Map<String, Object> rowStats = ImmutableMap.of("determinePartitions", determinePartitionsMap);
    IngestionStatsAndErrorsTaskReportData payload =
        new IngestionStatsAndErrorsTaskReportData(IngestionState.COMPLETED, null, rowStats, "An error");

    TaskReport taskReport = EasyMock.createMock(TaskReport.class);
    TaskStatusPlus taskStatusPlus = EasyMock.createMock(TaskStatusPlus.class);
    EasyMock.expect(taskReport.getPayload()).andReturn(payload).anyTimes();
    EasyMock.replay(taskReport, taskStatusPlus);

    TaskBasedJobStatus taskBasedJobStatus = new TaskBasedJobStatus("taskId", taskStatusPlus, taskReport);
    String message = taskBasedJobStatus.getUserFacingMessage();
    Assert.assertTrue(message.startsWith("Error: An error"));

    EasyMock.verify(taskReport, taskStatusPlus);
  }

  @Test
  public void testTaskBasedJobStatusWithNoError()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 10, "processedWithError", 0, "unparseable", 0);
    Map<String, Object> rowStats = ImmutableMap.of("determinePartitions", determinePartitionsMap);
    IngestionStatsAndErrorsTaskReportData payload =
        new IngestionStatsAndErrorsTaskReportData(IngestionState.COMPLETED, null, rowStats, null);

    TaskReport taskReport = EasyMock.createMock(TaskReport.class);
    TaskStatusPlus taskStatusPlus = EasyMock.createMock(TaskStatusPlus.class);
    EasyMock.expect(taskReport.getPayload()).andReturn(payload).anyTimes();
    EasyMock.replay(taskReport, taskStatusPlus);

    TaskBasedJobStatus taskBasedJobStatus = new TaskBasedJobStatus("taskId", taskStatusPlus, taskReport);
    String message = taskBasedJobStatus.getUserFacingMessage();
    Assert.assertTrue(!message.startsWith("Error:"));

    EasyMock.verify(taskReport, taskStatusPlus);
  }


  @Test
  public void testNullTaskReport()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 10, "processedWithError", 0, "unparseable", 0);
    Map<String, Object> rowStats = ImmutableMap.of("determinePartitions", determinePartitionsMap);
    IngestionStatsAndErrorsTaskReportData payload =
        new IngestionStatsAndErrorsTaskReportData(IngestionState.COMPLETED, null, rowStats, null);

    TaskReport taskReport = null;
    TaskStatusPlus taskStatusPlus = EasyMock.createMock(TaskStatusPlus.class);
    EasyMock.replay(taskStatusPlus);

    TaskBasedJobStatus taskBasedJobStatus = new TaskBasedJobStatus("taskId", taskStatusPlus, taskReport);
    String message = taskBasedJobStatus.getUserFacingMessage();
    Assert.assertTrue(message == null);

    EasyMock.verify(taskStatusPlus);
  }

  @Test
  public void testSchemaHint()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 0, "processedWithError", 0, "unparseable", 10);
    Map<String, Object> rowStats = ImmutableMap.of("determinePartitions", determinePartitionsMap);
    IngestionStatsAndErrorsTaskReportData payload =
        new IngestionStatsAndErrorsTaskReportData(IngestionState.COMPLETED, null, rowStats, null);

    TaskReport taskReport = EasyMock.createMock(TaskReport.class);
    TaskStatusPlus taskStatusPlus = EasyMock.createMock(TaskStatusPlus.class);
    EasyMock.expect(taskReport.getPayload()).andReturn(payload).anyTimes();
    EasyMock.replay(taskReport, taskStatusPlus);

    TaskBasedJobStatus taskBasedJobStatus = new TaskBasedJobStatus("taskId", taskStatusPlus, taskReport);
    String message = taskBasedJobStatus.getUserFacingMessage();
    Assert.assertTrue(message.contains("Hint: verify that the inputFormat"));

    EasyMock.verify(taskReport, taskStatusPlus);
  }

  @Test
  public void testInputFileHint()
  {
    Map<String, Integer> determinePartitionsMap =
        ImmutableMap.of("processed", 0, "processedWithError", 0, "unparseable", 0);
    Map<String, Object> rowStats = ImmutableMap.of("determinePartitions", determinePartitionsMap);
    IngestionStatsAndErrorsTaskReportData payload =
        new IngestionStatsAndErrorsTaskReportData(IngestionState.COMPLETED, null, rowStats, null);

    TaskReport taskReport = EasyMock.createMock(TaskReport.class);
    TaskStatusPlus taskStatusPlus = EasyMock.createMock(TaskStatusPlus.class);
    EasyMock.expect(taskReport.getPayload()).andReturn(payload).anyTimes();
    EasyMock.replay(taskReport, taskStatusPlus);

    TaskBasedJobStatus taskBasedJobStatus = new TaskBasedJobStatus("taskId", taskStatusPlus, taskReport);
    String message = taskBasedJobStatus.getUserFacingMessage();
    Assert.assertTrue(message.contains("Hint: verify that the input file exists"));

    EasyMock.verify(taskReport, taskStatusPlus);
  }
}
