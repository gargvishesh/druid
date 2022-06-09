/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.indexing.MSQCountersSnapshot;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Client for Talaria workers. Used by the Leader (Controller) task.
 */
public interface WorkerClient extends AutoCloseable
{
  ListenableFuture<Void> postWorkOrder(String workerId, WorkOrder workOrder);

  ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  );

  ListenableFuture<Void> postFinish(String workerId);

  ListenableFuture<MSQCountersSnapshot> getCounters(String workerId);

  ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId);

  ReadableFrameChannel getChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      ExecutorService connectExec
  );

  @Override
  void close() throws IOException;
}
