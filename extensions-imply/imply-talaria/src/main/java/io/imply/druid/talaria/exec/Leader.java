/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.talaria.counters.CounterSnapshots;
import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.indexing.MSQControllerTask;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.indexing.error.WorkerFailedFault;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskReport;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Interface for the leader executor. At present, Talaria can run in both the
 * Indexer and a Talaria server. This class, and its associated "context" allows the
 * Talaria code to run in both environments. This is an interface so that tests
 * can easily create test-fixture versions that do controlled subsets of functionality.
 */
public interface Leader
{
  // TODO(paul): Flesh out this status and move to the API package.
  // This probably wants to be an Async API status
  class RunningLeaderStatus
  {
    private final String id;

    @JsonCreator
    public RunningLeaderStatus(String id)
    {
      this.id = id;
    }

    @JsonProperty("id")
    public String getId()
    {
      return id;
    }
  }

  /**
   * Unique task/query ID for the batch query run by this leader.
   */
  String id();

  /**
   * The task which this leader runs.
   */
  MSQControllerTask task();

  /**
   * Runs the leader in the current thread. Surrounding classes provide
   * the execution thread.
   */
  TaskStatus run() throws Exception;

  /**
   * Returns a status update for a running leader. Not valid before
   * the task starts, or after completion.
   *
   * @return the active-task status, or {@code null} if the leader has
   * not started or has already completed.
   */
  RunningLeaderStatus status();

  /**
   * Terminate the query DAG upon a cancellation request.
   */
  void stopGracefully();

  // Worker-to-leader messages

  /**
   * Provide a {@link ClusterByStatisticsSnapshot} for shuffling stages.
   */
  void updateStatus(int stageNumber, int workerNumber, Object keyStatisticsObject);

  /**
   * System error reported by a subtask. Note that the errors are organized by
   * taskId, not by query/stage/worker, because system errors are associated
   * with a task rather than a specific query/stage/worker execution context.
   */
  void workerError(MSQErrorReport errorReport);

  /**
   * System warning reported by a subtask. Indicates that the worker has encountered a non-lethal error. Worker should
   * continue its execution in such a case. If the worker wants to report an error and stop its execution,
   * please use {@link Leader#workerError}
   */
  void workerWarning(List<MSQErrorReport> errorReports);

  /**
   * Periodic update of {@link CounterSnapshots} for a specific worker task.
   * Indicates a hard failure of the worker: fatal exception, the worker's
   * host dropped out of ZK, etc. The worker should be presumed dead. Restart
   * a new one if possible. If, due to a split network, the worker does
   * send subsequent message, the leader must return a status saying that
   * the worker is zombie and should stop execution immediately.
   * <p>
   * Due to inherent race conditions, it is possible that a worker fails
   * after it has successfully completed its work. In this case, the leader
   * can ignore the message, but must not attempt to later communicate with
   * the worker to, say, submit another work order: the worker will be gone.
   *
   * Till the fault tolerance is properly figured out for Talaria, if a worker encounters an error, all the worker nodes
   * and the stages that they are executing are marked as failed.
   */
  void workerFailed(WorkerFailedFault fault);

  /**
   * Periodic update of {@link CounterSnapshots} from subtasks.
   */
  void updateCounters(CounterSnapshotsTree snapshotsTree);

  /**
   * Reports that results are ready for a subtask.
   */
  void resultsComplete(
      String queryId,
      int stageNumber,
      int workerNumber,
      Object resultObject);

  /**
   * Returns the current list of task ids, ordered by worker number. The Nth task has worker number N.
   */
  List<String> getTaskIds();

  @Nullable
  Map<String, TaskReport> liveReports();

}
