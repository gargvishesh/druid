/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.imply.druid.talaria.exec.LeaderContext;
import io.imply.druid.talaria.exec.WorkerManagerClient;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TaskStartTimeoutFault;
import io.imply.druid.talaria.util.FutureUtils;
import io.imply.druid.talaria.util.TalariaContext;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Like {@link org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor}, but different.
 */
public class TalariaWorkerTaskLauncher
{
  private static final Logger log = new Logger(TalariaWorkerTaskLauncher.class);
  private static final long HIGH_FREQUENCY_CHECK_MILLIS = 100;
  private static final long LOW_FREQUENCY_CHECK_MILLIS = 2000;
  private static final long SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS = 10000;

  private final String controllerTaskId;
  private final String dataSource;
  private final LeaderContext context;
  private final int numTasks;
  private final ExecutorService exec;
  private final long maxTaskStartDelayMillis;
  private final boolean durableStageStorageEnabled;

  // Mutable state meant to be accessible by threads outside the main loop.
  // private final LinkedBlockingDeque<Runnable> mailbox = new LinkedBlockingDeque<>();
  private final SettableFuture<MSQTaskList> startFuture = SettableFuture.create();
  private final SettableFuture<Map<String, TaskState>> stopFuture = SettableFuture.create();
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final CountDownLatch stopLatch = new CountDownLatch(1);

  // Mutable state meant to be accessible only to the main loop. LinkedHashMap since order of key set matters.
  private final Map<String, TaskState> tasks = new LinkedHashMap<>();

  // Map of task id to start time. Used to check if tasks take too long to start.
  // Only used in the main loop.
  private final Map<String, Long> startTimeMillis = new TreeMap<>();

  // Set of tasks which are issued a cancel request by the leader.
  private final Set<String> canceledWorkerTasks = ConcurrentHashMap.newKeySet();

  public TalariaWorkerTaskLauncher(
      final String controllerTaskId,
      final String dataSource,
      final LeaderContext context,
      final int numTasks,
      final boolean durableStageStorageEnabled,
      final long maxTaskStartDelayMillis
  )
  {
    this.controllerTaskId = controllerTaskId;
    this.dataSource = dataSource;
    this.context = context;
    this.exec = Execs.singleThreaded("multi-stage-query-task-launcher[" + StringUtils.encodeForFormat(controllerTaskId) + "]-%s");
    this.numTasks = numTasks;
    this.durableStageStorageEnabled = durableStageStorageEnabled;
    this.maxTaskStartDelayMillis = maxTaskStartDelayMillis;
  }

  /**
   * Launches tasks, blocking until they are all in RUNNING state. Returns a future that resolves to the collective
   * and final state of the tasks once they are all done.
   */
  public ListenableFuture<Map<String, TaskState>> start()
  {
    if (started.compareAndSet(false, true)) {
      exec.submit(() -> {
        try {
          mainLoop();
        }
        catch (Throwable e) {
          log.warn(e, "Error encountered in main loop. Abandoning worker tasks.");
        }
      });
    }

    // Block until started, then return an "everything is done" future.
    FutureUtils.getUnchecked(startFuture, true);
    return stopFuture;
  }

  /**
   * Stops all tasks, blocking until they exit. Returns quietly if the tasks exit normally; throws an exception
   * if something else happens.
   */
  public void stop()
  {
    if (!started.get()) {
      throw new ISE("Not started");
    }

    if (stopped.compareAndSet(false, true)) {
      stopLatch.countDown();
      exec.shutdown();
    }

    // Block until stopped.
    FutureUtils.getUnchecked(stopFuture, true);
  }

  public Optional<MSQTaskList> getTaskList()
  {
    if (startFuture.isDone()) {
      return Optional.of(FutureUtils.getUncheckedImmediately(startFuture));
    } else {
      return Optional.empty();
    }
  }

  public boolean isFinished()
  {
    return stopFuture.isDone();
  }

  private void mainLoop()
  {
    try {
      final boolean startedOk = doStart();

      if (startedOk) {
        waitForTasksToFinish(true);
      }

      shutdownRemainingTasks();
      waitForTasksToFinish(false);

      if (startedOk) {
        stopFuture.set(ImmutableMap.copyOf(tasks));
      } else {
        // Doesn't really matter what we set stopFuture to here, because nobody will ever see it.
        // (When startedOk = false, "start" would have thrown an exception rather than returning stopFuture.)
        stopFuture.setException(new ISE("Error while starting"));
      }
    }
    catch (Throwable e) {
      if (!stopFuture.isDone()) {
        stopFuture.setException(e);
      }
    }
  }

  private boolean doStart()
  {
    try {
      if (startAllTasks() && waitForTasksToLaunch()) {
        startFuture.set(new MSQTaskList(ImmutableList.copyOf(tasks.keySet())));
        return true;
      } else {
        startFuture.setException(new ISE("stop() called before start() finished"));
        return false;
      }
    }
    catch (Throwable e) {
      startFuture.setException(e);
      return false;
    }
  }

  /**
   * Starts all tasks. Returns true once that happens, or false if {@link #stop()} is called while launching. Throws
   * an error if tasks fail to launch.
   * <p>
   * As a side effect, updates {@link #tasks} with task IDs and null statuses.
   */
  private boolean startAllTasks()
  {
    if (!tasks.isEmpty()) {
      throw new ISE("Tasks cannot be started twice.");
    }

    Map<String, Object> taskContext = new HashMap<>();
    if (durableStageStorageEnabled) {
      taskContext.put(TalariaContext.CTX_DURABLE_SHUFFLE_STORAGE, durableStageStorageEnabled);
    }

    long taskStartTime = System.currentTimeMillis();
    for (int i = 0; i < numTasks; i++) {
      if (stopped.get()) {
        return false;
      }

      final TalariaWorkerTask task = new TalariaWorkerTask(
          controllerTaskId,
          dataSource,
          i,
          taskContext
      );

      tasks.put(task.getId(), null);
      startTimeMillis.put(task.getId(), taskStartTime);
      context.workerManager().run(task.getId(), task);
    }

    return true;
  }

  /**
   * Wait for all tasks to have a known location, or to finish. Returns true once that happens, or false if
   * {@link #stop()} is called while waiting.
   */
  private boolean waitForTasksToLaunch() throws InterruptedException
  {
    WorkerManagerClient workerManager = context.workerManager();
    final long waitStartTime = System.currentTimeMillis();

    while (!stopped.get()) {
      final long loopStartTime = System.currentTimeMillis();

      final Map<String, TaskStatus> statuses = workerManager.statuses(tasks.keySet());
      statuses.forEach((k, v) -> tasks.put(k, v.getStatusCode()));

      if (statuses.values().stream().anyMatch(status -> status.getStatusCode().isFailure())) {
        throw new ISE("Tasks failed to start up");
      }

      boolean allTasksAreRunningWithLocationOrFinished = true;
      for (final Map.Entry<String, TaskState> taskEntry : tasks.entrySet()) {
        if (stopped.get()) {
          return false;
        }

        final String taskId = taskEntry.getKey();
        final TaskState taskState = taskEntry.getValue();

        final boolean taskIsRunningWithLocationOrFinished =
            taskState != null
            && !taskState.isComplete()
            && !TaskLocation.unknown().equals(workerManager.location(taskId));

        if (taskIsRunningWithLocationOrFinished) {
          startTimeMillis.remove(taskId);
        } else {
          allTasksAreRunningWithLocationOrFinished = false;
          break;
        }
      }

      // If task has not finished or been assigned to a location to run for more than MAX_ACCEPTED_TASK_START_DELAY, fail the tasks.
      startTimeMillis.forEach((taskId, startTime) -> {
        long timeElapsedFromTaskStart = System.currentTimeMillis() - startTime;
        if (timeElapsedFromTaskStart > maxTaskStartDelayMillis) {
          // adding 1 to accomodate the controller
          throw new TalariaException(new TaskStartTimeoutFault(numTasks + 1));
        }
      });

      if (allTasksAreRunningWithLocationOrFinished) {
        return true;
      }

      // Sleep for a bit, maybe.
      final long loopDuration = System.currentTimeMillis() - loopStartTime;
      final long sleepTime;

      if (System.currentTimeMillis() - waitStartTime < SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS) {
        sleepTime = HIGH_FREQUENCY_CHECK_MILLIS - loopDuration;
      } else {
        sleepTime = LOW_FREQUENCY_CHECK_MILLIS - loopDuration;
      }

      sleep(sleepTime, false);
    }

    return false;
  }

  /**
   * Wait for all tasks to enter state SUCCESS, or for one of them to enter state FAILURE, or for {@link #stop()} to
   * be called (if "stoppable" is set to true). Returns quietly once any of these things happens. Throws an error
   * if Overlord API calls fail while waiting for tasks to finish.
   * <p>
   * As a side effect, updates {@link #tasks} with task statuses.
   */
  private void waitForTasksToFinish(final boolean stoppable) throws InterruptedException
  {
    while (!(stoppable && stopped.get())) {
      final long loopStartTime = System.currentTimeMillis();

      final Set<String> taskStatusesNeeded = new HashSet<>();
      for (final Map.Entry<String, TaskState> taskEntry : tasks.entrySet()) {
        if (!taskEntry.getValue().isComplete()) {
          taskStatusesNeeded.add(taskEntry.getKey());
        }
      }

      if (!taskStatusesNeeded.isEmpty()) {
        final Map<String, TaskStatus> statuses = context.workerManager().statuses(taskStatusesNeeded);
        statuses.forEach((k, v) -> tasks.put(k, v.getStatusCode()));
      }

      boolean allTasksAreSuccessful = true;
      for (final Map.Entry<String, TaskState> taskEntry : tasks.entrySet()) {
        if (taskEntry.getValue() == TaskState.FAILED) {
          // FAILURE encountered -> early return.
          return;
        } else if (taskEntry.getValue() != TaskState.SUCCESS) {
          allTasksAreSuccessful = false;
        }
      }

      if (allTasksAreSuccessful) {
        // All tasks successful -> return.
        return;
      }

      // Sleep for a bit, maybe.
      final long loopDuration = System.currentTimeMillis() - loopStartTime;
      sleep(LOW_FREQUENCY_CHECK_MILLIS - loopDuration, stoppable);
    }
  }

  public void shutdownRemainingTasks()
  {
    for (final Map.Entry<String, TaskState> taskEntry : tasks.entrySet()) {
      if (!taskEntry.getValue().isComplete()) {
        canceledWorkerTasks.add(taskEntry.getKey());
        context.workerManager().cancel(taskEntry.getKey());
      }
    }
  }

  private void sleep(final long sleepMillis, final boolean stoppable) throws InterruptedException
  {
    if (sleepMillis > 0) {
      if (stoppable) {
        //noinspection ResultOfMethodCallIgnored: the latch just helps us stop sleeping early
        stopLatch.await(sleepMillis, TimeUnit.MILLISECONDS);
      } else {
        Thread.sleep(sleepMillis);
      }
    } else {
      // No wait, but check interrupted status anyway.
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }
  }

  /**
   * Checks if the leader has canceled the input taskId. This method is used in {@link io.imply.druid.talaria.exec.LeaderImpl}
   * to figure out if the worker taskId is cancelled by the leader. If yes, the errors from that worker taskId our ignored
   * for the error reports.
   *
   * @return true if task is canceled by the leader else false
   */
  public boolean isTaskCanceledByLeader(String taskId)
  {
    return canceledWorkerTasks.contains(taskId);
  }
}
