/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;

import java.util.concurrent.ExecutorService;

/**
 * Wrapper around any {@link WorkerClient} that converts exceptions into {@link TalariaException}
 * with {@link WorkerRpcFailedFault}. Useful so each implementation of WorkerClient does not need to do this on
 * its own.
 */
public class ExceptionWrappingWorkerClient implements WorkerClient
{
  private final WorkerClient client;

  public ExceptionWrappingWorkerClient(final WorkerClient client)
  {
    this.client = Preconditions.checkNotNull(client, "client");
  }

  @Override
  public void postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    try {
      client.postWorkOrder(workerTaskId, workOrder);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(workerTaskId));
    }
  }

  @Override
  public void postResultPartitionBoundaries(
      final String workerTaskId,
      final StageId stageId,
      final ClusterByPartitions partitionBoundaries
  )
  {
    try {
      client.postResultPartitionBoundaries(workerTaskId, stageId, partitionBoundaries);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(workerTaskId));
    }
  }

  @Override
  public void postCleanupStage(String workerTaskId, StageId stageId)
  {
    try {
      client.postCleanupStage(workerTaskId, stageId);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(workerTaskId));
    }
  }

  @Override
  public void postFinish(String taskId)
  {
    try {
      client.postFinish(taskId);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(taskId));
    }
  }

  @Override
  public TalariaCountersSnapshot getCounters(String taskId)
  {
    try {
      return client.getCounters(taskId);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(taskId));
    }
  }

  @Override
  public ReadableFrameChannel getChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      ExecutorService connectExec
  )
  {
    try {
      return client.getChannelData(workerTaskId, stageId, partitionNumber, connectExec);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(workerTaskId));
    }
  }

  @Override
  public void close()
  {
    client.close();
  }
}
