/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.file.FrameFile;
import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class TalariaTestWorkerClient implements WorkerClient
{
  Map<String, Worker> inMemoryWorkers;

  public TalariaTestWorkerClient(Map<String, Worker> inMemoryWorkers)
  {
    this.inMemoryWorkers = inMemoryWorkers;
  }

  @Override
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    inMemoryWorkers.get(workerTaskId).postWorkOrder(workOrder);
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  )
  {
    try {
      inMemoryWorkers.get(workerTaskId).postResultPartitionBoundaries(
          partitionBoundaries,
          stageId.getQueryId(),
          stageId.getStageNumber()
      );
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "unable to post result partition boundaries to workers");
    }
  }

  @Override
  public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
  {
    inMemoryWorkers.get(workerTaskId).postCleanupStage(stageId);
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> postFinish(String taskId)
  {
    inMemoryWorkers.get(taskId).postFinish();
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<TalariaCountersSnapshot> getCounters(String taskId)
  {
    return Futures.immediateFuture(inMemoryWorkers.get(taskId).getCounters());
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
      InputStream inputStream = inMemoryWorkers.get(workerTaskId).readChannel(
          stageId.getQueryId(),
          stageId.getStageNumber(),
          partitionNumber,
          0
      );

      Path framePath = Files.createTempFile(
          StringUtils.format("temp-%s-", stageId.getStageNumber()), null
      );

      FileOutputStream outStream = new FileOutputStream(framePath.toFile());
      byte[] buffer = new byte[8 * 1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }
      outStream.flush();
      outStream.close();
      inputStream.close();
      return new ReadableFileFrameChannel(FrameFile.open(framePath.toFile(), FrameFile.Flag.DELETE_ON_CLOSE));
    }
    catch (Exception e) {
      throw new ISE(e, "Error reading frame file channel");
    }

  }

  @Override
  public void close()
  {
    inMemoryWorkers.forEach((k, v) -> v.stopGracefully());
  }
}
