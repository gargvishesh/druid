/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.shuffle;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.indexing.InputChannelFactory;
import io.imply.druid.talaria.kernel.StageId;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;
import java.util.function.Supplier;

/**
 * Provides input channels connected to workers via {@link WorkerClient#fetchChannelData}.
 */
public class WorkerInputChannelFactory implements InputChannelFactory
{
  private final WorkerClient workerClient;
  private final Supplier<List<String>> taskList;

  public WorkerInputChannelFactory(final WorkerClient workerClient, final Supplier<List<String>> taskList)
  {
    this.workerClient = Preconditions.checkNotNull(workerClient, "workerClient");
    this.taskList = Preconditions.checkNotNull(taskList, "taskList");
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber)
  {
    final String taskId = taskList.get().get(workerNumber);
    final ReadableByteChunksFrameChannel channel =
        ReadableByteChunksFrameChannel.create(makeChannelId(taskId, stageId, partitionNumber));
    fetch(taskId, stageId, partitionNumber, 0, channel);
    return channel;
  }

  /**
   * Start a fetch chain for a particular channel starting at a particular offset.
   */
  private void fetch(
      final String taskId,
      final StageId stageId,
      final int partitionNumber,
      final long offset,
      final ReadableByteChunksFrameChannel channel
  )
  {
    final ListenableFuture<Boolean> fetchFuture =
        workerClient.fetchChannelData(taskId, stageId, partitionNumber, offset, channel);

    Futures.addCallback(
        fetchFuture,
        new FutureCallback<Boolean>()
        {
          @Override
          public void onSuccess(final Boolean lastFetch)
          {
            if (lastFetch) {
              channel.doneWriting();
            } else {
              fetch(taskId, stageId, partitionNumber, channel.getBytesAdded(), channel);
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            channel.setError(t);
          }
        }
    );
  }

  private static String makeChannelId(final String workerTaskId, final StageId stageId, final int partitionNumber)
  {
    return StringUtils.format("%s:%s:%s", workerTaskId, stageId, partitionNumber);
  }
}
