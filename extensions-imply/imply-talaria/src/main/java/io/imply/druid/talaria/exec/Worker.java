/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;
import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
import org.apache.druid.indexer.TaskStatus;

import javax.ws.rs.core.Response;

public interface Worker
{
  /**
   * Unique ID for this worker.
   */
  String id();

  /**
   * The task which this worker runs.
   */
  TalariaWorkerTask task();

  /**
   * Runs the worker in the current thread. Surrounding classes provide
   * the execution thread.
   */
  TaskStatus run() throws Exception;

  /**
   * Terminate the worker upon a cancellation request.
   */
  void stopGracefully();

  /**
   * Report that the leader has failed: a hard fault or the leader's
   * host dropped out of ZK. The worker must cease work immediately.
   * Cleanup then exit. Do not send final messages to the leader:
   * there will be no one home at the other end.
   */
  void leaderFailed();

  // Leader-to-worker, and worker-to-worker messages

  /**
   * Called when the worker chat handler receives a request for a work order. Accepts the work order and schedules it for
   * execution
   */
  void postWorkOrder(WorkOrder workOrder);

  /**
   * Called when the worker chat handler recieves the result partition boundaries for a particular stageNumber
   * and queryId
   */
  boolean postResultPartitionBoundaries(
      Object stagePartitionBoundariesObject,
      String queryId,
      int stageNumber
  );

  /**
   * Returns a response object containing the worker output for a particular queryId, stageNumber and partitionNumber.
   * Offset indicates the number of bytes to skip the channel data, and is used to prevent re-reading the same data
   * during retry in case of a connection error
   */
  Response readChannel(
      String queryId,
      int stageNumber,
      int partitionNumber,
      long offset
  );

  /**
   * Returns the snapshot of the worker counters
   */
  TalariaCountersSnapshot getCounters();

  /**
   * Called when the worker receives a POST request to clean up the stage with stageId, and is no longer required.
   * This marks the stage as FINISHED in its stage kernel, cleans up the worker output for the stage and optionally
   * frees any resources held on by the worker for the particular stage
   */
  void postCleanupStage(StageId stageId);

  /**
   * Called when the work required for the query has been finished
   */
  void postFinish();
}
