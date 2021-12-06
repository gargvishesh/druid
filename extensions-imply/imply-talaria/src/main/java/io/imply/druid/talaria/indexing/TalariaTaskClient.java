/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.frame.file.FrameFileHttpResponseHandler;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class TalariaTaskClient extends IndexTaskClient
{
  private static final int NUM_THREADS = 4;

  TalariaTaskClient(
      HttpClient httpClient,
      ObjectMapper objectMapper,
      TaskInfoProvider taskInfoProvider,
      Duration httpTimeout,
      String callerId,
      long numRetries
  )
  {
    super(
        httpClient,
        objectMapper,
        taskInfoProvider,
        httpTimeout,
        callerId,
        NUM_THREADS,
        numRetries
    );
  }

  /**
   * Client-side method for {@link TalariaControllerTask#httpPostKeyStatistics}.
   */
  public void postKeyStatistics(
      final String supervisorTaskId,
      final StageId stageId,
      final int workerNumber,
      final ClusterByStatisticsSnapshot keyStatistics
  )
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          supervisorTaskId,
          HttpMethod.POST,
          StringUtils.format(
              "keyStatistics/%s/%s/%d",
              StringUtils.urlEncode(stageId.getQueryId()),
              stageId.getStageNumber(),
              workerNumber
          ),
          null,
          serialize(keyStatistics),
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send report to supervisor task [%s]; HTTP response was [%s]",
            supervisorTaskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaControllerTask#httpPostCounters}.
   */
  public void postCounters(
      final String supervisorTaskId,
      final String taskId,
      final TalariaCountersSnapshot snapshot
  )
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          supervisorTaskId,
          HttpMethod.POST,
          StringUtils.format(
              "counters/%s",
              StringUtils.urlEncode(taskId)
          ),
          null,
          serialize(snapshot),
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send report to supervisor task [%s]; HTTP response was [%s]",
            supervisorTaskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaControllerTask#httpPostResultsComplete}.
   */
  public void postResultsComplete(
      final String supervisorTaskId,
      final StageId stageId,
      final int workerNumber,
      @Nullable final Object resultObject
  )
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          supervisorTaskId,
          HttpMethod.POST,
          StringUtils.format(
              "resultsComplete/%s/%s/%d",
              StringUtils.urlEncode(stageId.getQueryId()),
              stageId.getStageNumber(),
              workerNumber
          ),
          null,
          serialize(resultObject),
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send results-complete notification to supervisor task [%s]; HTTP response was [%s]",
            supervisorTaskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaControllerTask#httpPostWorkerError}.
   */
  public void postWorkerError(
      final String supervisorTaskId,
      final String taskId,
      final TalariaErrorReport errorWrapper
  )
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          supervisorTaskId,
          HttpMethod.POST,
          StringUtils.format("workerError/%s", StringUtils.urlEncode(taskId)),
          null,
          serialize(errorWrapper),
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send system error to supervisor task [%s]; HTTP response was [%s]",
            supervisorTaskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaWorkerTask#httpPostWorkOrder}.
   */
  public void postWorkOrder(final String workerTaskId, final WorkOrder workOrder)
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          workerTaskId,
          HttpMethod.POST,
          StringUtils.format("workOrder"),
          null,
          serialize(workOrder),
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send report to task [%s]; HTTP response was [%s]",
            workerTaskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaWorkerTask#httpPostResultPartitionBoundaries}.
   */
  public void postResultPartitionBoundaries(
      final String workerTaskId,
      final StageId stageId,
      final ClusterByPartitions partitionBoundaries
  )
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          workerTaskId,
          HttpMethod.POST,
          StringUtils.format(
              "resultPartitionBoundaries/%s/%d",
              StringUtils.urlEncode(stageId.getQueryId()),
              stageId.getStageNumber()
          ),
          null,
          serialize(partitionBoundaries),
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send report to task [%s]; HTTP response was [%s]",
            workerTaskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaWorkerTask#httpPostFinish}.
   */
  public void postFinish(final String taskId)
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          taskId,
          HttpMethod.POST,
          "finish",
          null,
          ByteArrays.EMPTY_ARRAY,
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to post finish message to task [%s]; HTTP response was [%s]",
            taskId,
            response.getStatus()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaControllerTask#httpGetTaskList}.
   */
  public Optional<List<String>> getTaskList(final String supervisorTaskId)
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          supervisorTaskId,
          HttpMethod.GET,
          "taskList",
          null,
          ByteArrays.EMPTY_ARRAY,
          true
      );

      final TalariaTaskList retVal = deserialize(response.getContent(), TalariaTaskList.class);

      return Optional.ofNullable(retVal.getTaskIds());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaWorkerTask#httpGetCounters}.
   */
  public TalariaCountersSnapshot getCounters(final String taskId)
  {
    try {
      final StringFullResponseHolder response = submitJsonRequest(
          taskId,
          HttpMethod.GET,
          "counters",
          null,
          ByteArrays.EMPTY_ARRAY,
          true
      );

      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to get counters from task [%s]; HTTP response was [%s]",
            taskId,
            response.getStatus()
        );
      }

      return deserialize(response.getContent(), TalariaCountersSnapshot.class);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Client-side method for {@link TalariaWorkerTask#httpGetChannelData}.
   */
  public ReadableFrameChannel getChannelData(
      final String workerTaskId,
      final StageId stageId,
      final int partitionNumber,
      final ExecutorService connectExec
  )
  {
    final String path = StringUtils.format(
        "channels/%s/%d/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        partitionNumber
    );

    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.minimal();
    final TalariaFrameChannelConnectionManager connectionManager = new TalariaFrameChannelConnectionManager(
        StringUtils.format("%s:%s", workerTaskId, path),
        channel,
        connectExec
    );

    return connectionManager.connect(
        (connectionNumber, offset) ->
            submitRequest(
                workerTaskId,
                null,
                HttpMethod.GET,
                path,
                StringUtils.format("offset=%d", offset),
                ByteArrays.EMPTY_ARRAY,
                new FrameFileHttpResponseHandler(
                    channel,
                    e -> connectionManager.handleChannelException(connectionNumber, e)
                ),
                true
            )
    );
  }
}
