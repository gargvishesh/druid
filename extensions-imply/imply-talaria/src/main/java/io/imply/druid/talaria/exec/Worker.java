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

  // Leader-to-worker, and worker-to-worker messages

  void postWorkOrder(WorkOrder workOrder);
  boolean postResultPartitionBoundaries(
      Object stagePartitionBoundariesObject,
      String queryId,
      int stageNumber);
  Response readChannel(
      String queryId,
      int stageNumber,
      int partitionNumber,
      long offset);
  TalariaCountersSnapshot getCounters();
  void postFinish();
}
