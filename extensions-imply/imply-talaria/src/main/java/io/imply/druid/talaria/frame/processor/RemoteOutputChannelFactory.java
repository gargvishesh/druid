/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import io.imply.druid.storage.StorageConnector;
import io.imply.druid.talaria.frame.ArenaMemoryAllocator;
import io.imply.druid.talaria.frame.channel.WritableStreamFrameChannel;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.ExecutorService;

public class RemoteOutputChannelFactory implements OutputChannelFactory
{
  final String controllerTaskId;
  final String workerTaskId;
  final int stageNumber;

  final StorageConnector storageConnector;

  final ExecutorService executorService;

  public RemoteOutputChannelFactory(
      String controllerTaskId,
      String workerTaskId,
      int stageNumber,
      StorageConnector storageConnector,
      ExecutorService executorService
  )
  {
    this.controllerTaskId = controllerTaskId;
    this.workerTaskId = workerTaskId;
    this.stageNumber = stageNumber;
    this.storageConnector = storageConnector;
    this.executorService = executorService;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    final String fileName = getPartitionFileName(controllerTaskId, workerTaskId, stageNumber, partitionNumber);
    final WritableStreamFrameChannel writableChannel =
        new WritableStreamFrameChannel(
            FrameFileWriter.open(
                Channels.newChannel(storageConnector.write(fileName))
            )
        );

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(1_000_000),
        () -> null, // remote reads should happen by the IndexerWorkerClient#getChannelData
        partitionNumber
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    final String fileName = getPartitionFileName(controllerTaskId, workerTaskId, stageNumber, partitionNumber);
    // As tasks dependent on output of this partition will forever block if no file is present in RemoteStorage. Hence, writing a dummy frame.
    try {

      FrameFileWriter.open(Channels.newChannel(storageConnector.write(fileName))).close();
      return OutputChannel.nil(partitionNumber);
    }
    catch (IOException e) {
      throw new ISE(
          e,
          "Unable to create empty remote output of workerTask[%s] stage[%d] partition[%d]",
          workerTaskId,
          stageNumber,
          partitionNumber
      );
    }
  }

  public static String getPartitionFileName(
      String controllerTaskId,
      String workerTaskId,
      int stageNumber,
      int partitionNumber
  )
  {
    return StringUtils.format(
        "controller_%s/worker_%s/stage_%d/part_%d",
        controllerTaskId,
        workerTaskId,
        stageNumber,
        partitionNumber
    );
  }
}
