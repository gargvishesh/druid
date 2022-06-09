/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import io.imply.druid.talaria.indexing.TalariaWorkerTaskLauncher;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TaskStartTimeoutFault;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TalariaTasksTest
{
  private static final String CONTROLLER_ID = "controller-id";
  private static final String WORKER_ID = "worker-id";
  private static final String CONTROLLER_HOST = "controller-host";
  private static final String WORKER_HOST = "worker-host";

  @Test
  public void test_makeErrorReport_allNull()
  {
    Assert.assertEquals(
        TalariaErrorReport.fromFault(
            CONTROLLER_ID,
            CONTROLLER_HOST,
            null,
            UnknownFault.forMessage(TalariaTasks.GENERIC_QUERY_FAILED_MESSAGE)
        ),
        TalariaTasks.makeErrorReport(CONTROLLER_ID, CONTROLLER_HOST, null, null)
    );
  }

  @Test
  public void test_makeErrorReport_controllerOnly()
  {
    final TalariaErrorReport controllerReport = TalariaTasks.makeErrorReport(
        CONTROLLER_ID,
        CONTROLLER_HOST,
        TalariaErrorReport.fromFault(CONTROLLER_ID, CONTROLLER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(controllerReport, TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, null));
  }

  @Test
  public void test_makeErrorReport_workerOnly()
  {
    final TalariaErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(workerReport, TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, null, workerReport));
  }

  @Test
  public void test_makeErrorReport_controllerPreferred()
  {
    final TalariaErrorReport controllerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyWorkersFault(2, 20)),
        null
    );

    final TalariaErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        controllerReport,
        TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }

  @Test
  public void test_makeErrorReport_workerPreferred()
  {
    final TalariaErrorReport controllerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new WorkerRpcFailedFault(WORKER_ID)),
        null
    );

    final TalariaErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        workerReport,
        TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }

  @Test
  public void test_queryWithoutEnoughSlots_shouldThrowException()
  {
    final int numSlots = 5;
    final int numTasks = 10;

    LeaderContext leaderContext = mock(LeaderContext.class);
    when(leaderContext.workerManager()).thenReturn(new TasksTestWorkerManagerClient(numSlots));
    TalariaWorkerTaskLauncher talariaWorkerTaskLauncher = new TalariaWorkerTaskLauncher(
        CONTROLLER_ID,
        "foo",
        leaderContext,
        numTasks,
        false,
        TimeUnit.SECONDS.toMillis(5)
    );

    try {
      talariaWorkerTaskLauncher.start();
      fail();
    }
    catch (Exception e) {
      Assert.assertEquals(
          ((TalariaException) e.getCause().getCause()).getFault().getCodeWithMessage(),
          new TaskStartTimeoutFault(numTasks + 1).getCodeWithMessage()
      );
    }
  }

  static class TasksTestWorkerManagerClient implements WorkerManagerClient
  {
    // Num of slots available for tasks
    final int numSlots;

    public TasksTestWorkerManagerClient(int numSlots)
    {
      this.numSlots = numSlots;
    }

    Set<String> taskSlots;

    @Override
    public TaskLocation location(String workerId)
    {
      if (taskSlots.contains(workerId)) {
        return TaskLocation.create("host", 80, 23);
      } else {
        return TaskLocation.unknown();
      }
    }

    @Override
    public Map<String, TaskStatus> statuses(Set<String> taskIds)
    {
      // Choose numSlots taskIds from the set and add it to taskSlots
      if (taskSlots == null) {
        taskSlots = taskIds.stream()
                           .limit(numSlots)
                           .collect(Collectors.toSet());
      }

      return taskSlots.stream()
                      .collect(
                          Collectors.toMap(
                              taskId -> taskId,
                              taskId -> new TaskStatus(taskId, TaskState.RUNNING, 2)
                          )
                      );
    }

    @Override
    public String run(String leaderId, TalariaWorkerTask task)
    {
      return null;
    }

    @Override
    public void cancel(String workerId)
    {
      // do nothing
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }
}
