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
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.indexing.MSQCountersSnapshot;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;

import java.io.IOException;

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

  /**
   * Fetch some data from a worker and add it to the provided channel. The exact amount of data is determined
   * by the server.
   *
   * Returns a future that resolves to true (no more data left), false (there is more data left), or exception (some
   * kind of unrecoverable exception).
   */
  ListenableFuture<Boolean> fetchChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  );

  @Override
  void close() throws IOException;
}
