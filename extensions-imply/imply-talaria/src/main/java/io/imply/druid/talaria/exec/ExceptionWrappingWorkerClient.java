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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;

import javax.annotation.Nullable;
import java.io.IOException;

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
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    return wrap(workerTaskId, client, c -> c.postWorkOrder(workerTaskId, workOrder));
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      final String workerTaskId,
      final StageId stageId,
      final ClusterByPartitions partitionBoundaries
  )
  {
    return wrap(workerTaskId, client, c -> c.postResultPartitionBoundaries(workerTaskId, stageId, partitionBoundaries));
  }

  @Override
  public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
  {
    return wrap(workerTaskId, client, c -> c.postCleanupStage(workerTaskId, stageId));
  }

  @Override
  public ListenableFuture<Void> postFinish(String workerTaskId)
  {
    return wrap(workerTaskId, client, c -> c.postFinish(workerTaskId));
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(String workerTaskId)
  {
    return wrap(workerTaskId, client, c -> c.getCounters(workerTaskId));
  }

  @Override
  public ListenableFuture<Boolean> fetchChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  )
  {
    return wrap(workerTaskId, client, c -> c.fetchChannelData(workerTaskId, stageId, partitionNumber, offset, channel));
  }

  @Override
  public void close() throws IOException
  {
    client.close();
  }

  private static <T> ListenableFuture<T> wrap(
      final String workerTaskId,
      final WorkerClient client,
      final ClientFn<T> clientFn
  )
  {
    final SettableFuture<T> retVal = SettableFuture.create();
    final ListenableFuture<T> clientFuture;

    try {
      clientFuture = clientFn.apply(client);
    }
    catch (Exception e) {
      throw new TalariaException(e, new WorkerRpcFailedFault(workerTaskId));
    }

    Futures.addCallback(
        clientFuture,
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(@Nullable T result)
          {
            retVal.set(result);
          }

          @Override
          public void onFailure(Throwable t)
          {
            retVal.setException(new TalariaException(t, new WorkerRpcFailedFault(workerTaskId)));
          }
        }
    );

    return retVal;
  }

  private interface ClientFn<T>
  {
    ListenableFuture<T> apply(WorkerClient client);
  }
}
