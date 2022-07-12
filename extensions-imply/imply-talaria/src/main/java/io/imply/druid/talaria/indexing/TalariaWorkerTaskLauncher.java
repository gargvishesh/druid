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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.exec.LeaderContext;
import io.imply.druid.talaria.exec.WorkerManagerClient;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TaskStartTimeoutFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.error.WorkerFailedFault;
import io.imply.druid.talaria.util.TalariaContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * Like {@link org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor}, but different.
 */
public class TalariaWorkerTaskLauncher
{
  private static final Logger log = new Logger(TalariaWorkerTaskLauncher.class);
  private static final long HIGH_FREQUENCY_CHECK_MILLIS = 100;
  private static final long LOW_FREQUENCY_CHECK_MILLIS = 2000;
  private static final long SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS = 10000;
  private static final long SHUTDOWN_TIMEOUT_MS = Duration.ofMinutes(1).toMillis();

  // States for "state" variable.
  private enum State
  {
    NEW,
    STARTED,
    STOPPED
  }

  private final String controllerTaskId;
  private final String dataSource;
  private final LeaderContext context;
  private final ExecutorService exec;
  private final long maxTaskStartDelayMillis;
  private final boolean durableStageStorageEnabled;

  // Mutable state meant to be accessible by threads outside the main loop.
  private final SettableFuture<Map<String, TaskState>> stopFuture = SettableFuture.create();
  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
  private final AtomicBoolean cancelTasksOnStop = new AtomicBoolean();

  @GuardedBy("taskIds")
  private int desiredTaskCount = 0;

  // Worker number -> task ID.
  @GuardedBy("taskIds")
  private final List<String> taskIds = new ArrayList<>();

  // Worker number -> whether the task has fully started up or not.
  @GuardedBy("taskIds")
  private final IntSet fullyStartedTasks = new IntOpenHashSet();

  // Mutable state accessible only to the main loop. LinkedHashMap since order of key set matters. Tasks are added
  // here once they are submitted for running, but before they are fully started up.
  private final Map<String, TaskTracker> taskTrackers = new LinkedHashMap<>();

  // Set of tasks which are issued a cancel request by the leader.
  private final Set<String> canceledWorkerTasks = ConcurrentHashMap.newKeySet();

  public TalariaWorkerTaskLauncher(
      final String controllerTaskId,
      final String dataSource,
      final LeaderContext context,
      final boolean durableStageStorageEnabled,
      final long maxTaskStartDelayMillis
  )
  {
    this.controllerTaskId = controllerTaskId;
    this.dataSource = dataSource;
    this.context = context;
    this.exec = Execs.singleThreaded(
        "multi-stage-query-task-launcher[" + StringUtils.encodeForFormat(controllerTaskId) + "]-%s"
    );
    this.durableStageStorageEnabled = durableStageStorageEnabled;
    this.maxTaskStartDelayMillis = maxTaskStartDelayMillis;
  }

  /**
   * Launches tasks, blocking until they are all in RUNNING state. Returns a future that resolves to the collective
   * and final state of the tasks once they are all done.
   */
  public ListenableFuture<Map<String, TaskState>> start()
  {
    if (state.compareAndSet(State.NEW, State.STARTED)) {
      exec.submit(() -> {
        try {
          mainLoop();
        }
        catch (Throwable e) {
          log.warn(e, "Error encountered in main loop. Abandoning worker tasks.");
        }
      });
    }

    // Return an "everything is done" future that callers can wait for.
    return stopFuture;
  }

  /**
   * Stops all tasks, blocking until they exit. Returns quietly if the tasks exit normally; throws an exception
   * if something else happens.
   */
  public void stop(final boolean interrupt)
  {
    if (state.compareAndSet(State.STARTED, State.STOPPED)) {
      if (interrupt) {
        cancelTasksOnStop.set(true);
      }

      synchronized (taskIds) {
        // Wake up sleeping mainLoop.
        taskIds.notifyAll();
      }

      // Only shutdown the executor when transitioning from STARTED.
      exec.shutdown();
    } else if (state.get() == State.STOPPED) {
      // interrupt = true is sticky: don't reset on interrupt = false.
      if (interrupt) {
        cancelTasksOnStop.set(true);
      }
    } else {
      throw new ISE("Cannot stop(%s) from state [%s]", interrupt, state.get());
    }

    // Block until stopped.
    FutureUtils.getUnchecked(stopFuture, true);
  }

  /**
   * Get the list of currently-active tasks.
   */
  public List<String> getTaskList()
  {
    synchronized (taskIds) {
      return ImmutableList.copyOf(taskIds);
    }
  }

  /**
   * Launch additional tasks, if needed, to bring the size of {@link #taskIds} up to {@code taskCount}. If enough
   * tasks are already running, this method does nothing.
   */
  public void launchTasksIfNeeded(final int taskCount) throws InterruptedException
  {
    synchronized (taskIds) {
      if (taskCount > desiredTaskCount) {
        desiredTaskCount = taskCount;
      }

      while (taskIds.size() < taskCount || !IntStream.range(0, taskCount).allMatch(fullyStartedTasks::contains)) {
        if (stopFuture.isDone() || stopFuture.isCancelled()) {
          FutureUtils.getUnchecked(stopFuture, false);
          throw new ISE("Stopped");
        }

        taskIds.wait();
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

  private void mainLoop()
  {
    try {
      Throwable caught = null;

      while (state.get() == State.STARTED) {
        final long loopStartTime = System.currentTimeMillis();

        try {
          runNewTasks();
          updateTaskTrackersAndTaskIds();
          checkForErroneousTasks();
        }
        catch (Throwable e) {
          state.set(State.STOPPED);
          cancelTasksOnStop.set(true);

          if (!isWorkerFailedException(e)) {
            caught = e;
          }

          break;
        }

        // Sleep for a bit, maybe.
        sleep(computeSleepTime(System.currentTimeMillis() - loopStartTime), false);
      }

      // Only valid transition out of STARTED.
      assert state.get() == State.STOPPED;

      final long stopStartTime = System.currentTimeMillis();

      while (taskTrackers.values().stream().anyMatch(tracker -> !tracker.status.getStatusCode().isComplete())) {
        final long loopStartTime = System.currentTimeMillis();

        if (cancelTasksOnStop.get()) {
          shutDownTasks();
        }

        updateTaskTrackersAndTaskIds();

        // Sleep for a bit, maybe.
        final long now = System.currentTimeMillis();

        if (now > stopStartTime + SHUTDOWN_TIMEOUT_MS) {
          if (caught != null) {
            throw caught;
          } else {
            throw new ISE("Task shutdown timed out (limit = %,dms)", SHUTDOWN_TIMEOUT_MS);
          }
        }

        sleep(computeSleepTime(now - loopStartTime), true);
      }

      if (caught != null) {
        throw caught;
      }

      stopFuture.set(computeStopReturnValue());
    }
    catch (Throwable e) {
      if (!stopFuture.isDone()) {
        stopFuture.setException(e);
      }
    }

    synchronized (taskIds) {
      // notify taskIds so launchWorkersIfNeeded can wake up, if it is sleeping, and notice stopFuture is done.
      taskIds.notifyAll();
    }
  }

  /**
   * Used by the main loop to launch new tasks up to {@link #desiredTaskCount}. Adds trackers to {@link #taskTrackers}
   * for newly launched tasks.
   */
  private void runNewTasks()
  {
    final Map<String, Object> taskContext = new HashMap<>();

    if (durableStageStorageEnabled) {
      taskContext.put(TalariaContext.CTX_DURABLE_SHUFFLE_STORAGE, true);
    }

    final int firstTask;
    final int taskCount;

    synchronized (taskIds) {
      firstTask = taskIds.size();
      taskCount = desiredTaskCount;
    }

    for (int i = firstTask; i < taskCount; i++) {
      final TalariaWorkerTask task = new TalariaWorkerTask(
          controllerTaskId,
          dataSource,
          i,
          taskContext
      );

      taskTrackers.put(task.getId(), new TaskTracker(i));
      context.workerManager().run(task.getId(), task);

      synchronized (taskIds) {
        taskIds.add(task.getId());
        taskIds.notifyAll();
      }
    }
  }

  /**
   * Used by the main loop to update {@link #taskTrackers} and {@link #fullyStartedTasks}.
   */
  private void updateTaskTrackersAndTaskIds()
  {
    final Set<String> taskStatusesNeeded = new HashSet<>();
    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      if (taskEntry.getValue().status == null || !taskEntry.getValue().status.getStatusCode().isComplete()) {
        taskStatusesNeeded.add(taskEntry.getKey());
      }
    }

    if (!taskStatusesNeeded.isEmpty()) {
      final WorkerManagerClient workerManager = context.workerManager();
      final Map<String, TaskStatus> statuses = workerManager.statuses(taskStatusesNeeded);

      for (Map.Entry<String, TaskStatus> statusEntry : statuses.entrySet()) {
        final String taskId = statusEntry.getKey();
        final TaskTracker tracker = taskTrackers.get(taskId);
        tracker.status = statusEntry.getValue();

        if (!tracker.status.getStatusCode().isComplete() && tracker.unknownLocation()) {
          // Look up location if not known. Note: this location is not used to actually contact the task. For that,
          // we have SpecificTaskServiceLocator. This location is only used to determine if a task has started up.
          tracker.initialLocation = workerManager.location(taskId);
        }

        if (tracker.status.getStatusCode() == TaskState.RUNNING && !tracker.unknownLocation()) {
          synchronized (taskIds) {
            fullyStartedTasks.add(tracker.workerNumber);
            taskIds.notifyAll();
          }
        }
      }
    }
  }

  /**
   * Used by the main loop to generate exceptions if any tasks have failed, have taken too long to start up, or
   * have gone inexplicably missing.
   *
   * Throws an exception if some task is erroneous.
   */
  private void checkForErroneousTasks()
  {
    final int numTasks = taskTrackers.size();

    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();

      if (tracker.status == null) {
        throw new TalariaException(UnknownFault.forMessage(StringUtils.format("Task [%s] status missing", taskId)));
      }

      if (tracker.didRunTimeOut(maxTaskStartDelayMillis) && !canceledWorkerTasks.contains(taskId)) {
        throw new TalariaException(new TaskStartTimeoutFault(numTasks + 1));
      }

      if (tracker.didFail() && !canceledWorkerTasks.contains(taskId)) {
        throw new TalariaException(new WorkerFailedFault(taskId));
      }
    }
  }

  private void shutDownTasks()
  {
    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();
      if (!canceledWorkerTasks.contains(taskId)
          && (tracker.status == null || !tracker.status.getStatusCode().isComplete())) {
        canceledWorkerTasks.add(taskId);
        context.workerManager().cancel(taskId);
      }
    }
  }

  /**
   * Used by the main loop to populate {@link #stopFuture}.
   */
  private Map<String, TaskState> computeStopReturnValue()
  {
    final Map<String, TaskState> retVal = new LinkedHashMap<>();

    for (Map.Entry<String, TaskTracker> trackerEntry : taskTrackers.entrySet()) {
      final String taskId = trackerEntry.getKey();
      final TaskTracker tracker = trackerEntry.getValue();

      // Return status if known, FAILED if not known.
      retVal.put(taskId, tracker.status != null ? tracker.status.getStatusCode() : TaskState.FAILED);
    }

    return retVal;
  }

  /**
   * Used by the main loop to decide how often to check task status.
   */
  private long computeSleepTime(final long loopDurationMs)
  {
    final OptionalLong maxTaskStartTime =
        taskTrackers.values().stream().mapToLong(tracker -> tracker.startTimeMs).max();

    if (maxTaskStartTime.isPresent() &&
        System.currentTimeMillis() - maxTaskStartTime.getAsLong() < SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS) {
      return HIGH_FREQUENCY_CHECK_MILLIS - loopDurationMs;
    } else {
      return LOW_FREQUENCY_CHECK_MILLIS - loopDurationMs;
    }
  }

  private void sleep(final long sleepMillis, final boolean shuttingDown) throws InterruptedException
  {
    if (sleepMillis > 0) {
      if (shuttingDown) {
        Thread.sleep(sleepMillis);
      } else {
        // wait on taskIds so we can wake up early if needed.
        synchronized (taskIds) {
          taskIds.wait(sleepMillis);
        }
      }
    } else {
      // No wait, but check interrupted status anyway.
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }
  }

  private static boolean isWorkerFailedException(final Throwable e)
  {
    return e instanceof TalariaException
           && WorkerFailedFault.CODE.equals(((TalariaException) e).getFault().getErrorCode());
  }

  /**
   * Tracker for information about a worker. Mutable.
   */
  private static class TaskTracker
  {
    private final int workerNumber;
    private final long startTimeMs = System.currentTimeMillis();
    private TaskStatus status;
    private TaskLocation initialLocation;

    public TaskTracker(int workerNumber)
    {
      this.workerNumber = workerNumber;
    }

    public boolean unknownLocation()
    {
      return initialLocation == null || TaskLocation.unknown().equals(initialLocation);
    }

    public boolean didFail()
    {
      return status != null && status.getStatusCode().isFailure();
    }

    public boolean didRunTimeOut(final long maxTaskStartDelayMillis)
    {
      return (status == null || status.getStatusCode() == TaskState.RUNNING)
             && unknownLocation()
             && System.currentTimeMillis() - startTimeMs > maxTaskStartDelayMillis;
    }
  }
}
