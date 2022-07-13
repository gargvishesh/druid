/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import io.imply.druid.talaria.indexing.TalariaWorkerTaskLauncher;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TaskStartTimeoutFault;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
        MSQErrorReport.fromFault(
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
    final MSQErrorReport controllerReport = TalariaTasks.makeErrorReport(
        CONTROLLER_ID,
        CONTROLLER_HOST,
        MSQErrorReport.fromFault(CONTROLLER_ID, CONTROLLER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(controllerReport, TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, null));
  }

  @Test
  public void test_makeErrorReport_workerOnly()
  {
    final MSQErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(workerReport, TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, null, workerReport));
  }

  @Test
  public void test_makeErrorReport_controllerPreferred()
  {
    final MSQErrorReport controllerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyWorkersFault(2, 20)),
        null
    );

    final MSQErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
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
    final MSQErrorReport controllerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new WorkerRpcFailedFault(WORKER_ID)),
        null
    );

    final MSQErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
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
        false,
        TimeUnit.SECONDS.toMillis(5)
    );

    try {
      talariaWorkerTaskLauncher.start();
      talariaWorkerTaskLauncher.launchTasksIfNeeded(numTasks);
      fail();
    }
    catch (Exception e) {
      Assert.assertEquals(
          new TaskStartTimeoutFault(numTasks + 1).getCodeWithMessage(),
          ((TalariaException) e.getCause()).getFault().getCodeWithMessage()
      );
    }
  }

  static class TasksTestWorkerManagerClient implements WorkerManagerClient
  {
    // Num of slots available for tasks
    final int numSlots;

    @GuardedBy("this")
    final Set<String> allTasks = new HashSet<>();

    @GuardedBy("this")
    final Set<String> runningTasks = new HashSet<>();

    @GuardedBy("this")
    final Set<String> canceledTasks = new HashSet<>();

    public TasksTestWorkerManagerClient(final int numSlots)
    {
      this.numSlots = numSlots;
    }

    @Override
    public synchronized Map<String, TaskStatus> statuses(final Set<String> taskIds)
    {
      final Map<String, TaskStatus> retVal = new HashMap<>();

      for (final String taskId : taskIds) {
        if (allTasks.contains(taskId)) {
          retVal.put(
              taskId,
              new TaskStatus(
                  taskId,
                  canceledTasks.contains(taskId) ? TaskState.FAILED : TaskState.RUNNING,
                  2,
                  null,
                  null
              )
          );
        }
      }

      return retVal;
    }

    @Override
    public synchronized TaskLocation location(String workerId)
    {
      if (runningTasks.contains(workerId)) {
        return TaskLocation.create("host-" + workerId, 1, -1);
      } else {
        return TaskLocation.unknown();
      }
    }

    @Override
    public synchronized String run(String leaderId, TalariaWorkerTask task)
    {
      allTasks.add(task.getId());

      if (runningTasks.size() < numSlots) {
        runningTasks.add(task.getId());
      }

      return task.getId();
    }

    @Override
    public synchronized void cancel(String workerId)
    {
      runningTasks.remove(workerId);
      canceledTasks.add(workerId);
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }
}
