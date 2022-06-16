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
import io.imply.druid.storage.StorageConnector;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableInputStreamFrameChannel;
import io.imply.druid.talaria.indexing.InputChannelFactory;
import io.imply.druid.talaria.kernel.StageId;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Provides input channels connected to durable storage.
 */
public class DurableStorageInputChannelFactory implements InputChannelFactory
{
  private final StorageConnector storageConnector;
  private final ExecutorService remoteInputStreamPool;
  private final String controllerTaskId;
  private final Supplier<List<String>> taskList;

  public DurableStorageInputChannelFactory(
      final String controllerTaskId,
      final Supplier<List<String>> taskList,
      final StorageConnector storageConnector,
      final ExecutorService remoteInputStreamPool
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.taskList = Preconditions.checkNotNull(taskList, "taskList");
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
    this.remoteInputStreamPool = Preconditions.checkNotNull(remoteInputStreamPool, "remoteInputStreamPool");
  }

  /**
   * Creates an instance that is the standard production implementation. Closeable items are registered with
   * the provided Closer.
   */
  public static DurableStorageInputChannelFactory createStandardImplementation(
      final String controllerTaskId,
      final Supplier<List<String>> taskList,
      final StorageConnector storageConnector,
      final Closer closer
  )
  {
    final ExecutorService remoteInputStreamPool =
        Executors.newCachedThreadPool(Execs.makeThreadFactory(controllerTaskId + "-remote-fetcher-%d"));
    closer.register(remoteInputStreamPool::shutdownNow);
    return new DurableStorageInputChannelFactory(controllerTaskId, taskList, storageConnector, remoteInputStreamPool);
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException
  {
    final String workerTaskId = taskList.get().get(workerNumber);

    try {
      final String remotePartitionPath = DurableStorageOutputChannelFactory.getPartitionFileName(
          controllerTaskId,
          workerTaskId,
          stageId.getStageNumber(),
          partitionNumber
      );
      RetryUtils.retry(() -> {
        if (!storageConnector.pathExists(remotePartitionPath)) {
          throw new ISE(
              "Could not find remote output of worker task[%s] stage[%d] partition[%d]",
              workerTaskId,
              stageId.getStageNumber(),
              partitionNumber
          );
        }
        return Boolean.TRUE;
      }, (throwable) -> true, 10);
      final InputStream inputStream = storageConnector.read(remotePartitionPath);

      ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
          inputStream,
          remotePartitionPath,
          remoteInputStreamPool
      );
      readableInputStreamFrameChannel.startReading();
      return readableInputStreamFrameChannel;
    }
    catch (Exception e) {
      throw new IOE(
          e,
          "Could not find remote output of worker task[%s] stage[%d] partition[%d]",
          workerTaskId,
          stageId.getStageNumber(),
          partitionNumber
      );
    }
  }
}
