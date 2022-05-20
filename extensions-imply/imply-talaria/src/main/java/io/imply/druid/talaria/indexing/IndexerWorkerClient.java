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
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.file.FrameFileHttpResponseHandler;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
import io.imply.druid.talaria.rpc.DruidServiceClient;
import io.imply.druid.talaria.rpc.DruidServiceClientFactory;
import io.imply.druid.talaria.rpc.RequestBuilder;
import io.imply.druid.talaria.rpc.RpcServerError;
import io.imply.druid.talaria.rpc.handler.IgnoreHttpResponseHandler;
import io.imply.druid.talaria.rpc.handler.JsonHttpResponseHandler;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import io.imply.druid.talaria.rpc.indexing.SpecificTaskRetryPolicy;
import io.imply.druid.talaria.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nonnull;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class IndexerWorkerClient implements WorkerClient
{
  private static final Duration HTTP_CHANNEL_TIMEOUT = Duration.standardMinutes(5);

  private final DruidServiceClientFactory clientFactory;
  private final OverlordServiceClient overlordClient;
  private final ObjectMapper jsonMapper;

  @GuardedBy("clientMap")
  private final Map<String, Pair<DruidServiceClient, Closeable>> clientMap = new HashMap<>();

  public IndexerWorkerClient(
      final DruidServiceClientFactory clientFactory,
      final OverlordServiceClient overlordClient,
      final ObjectMapper jsonMapper
  )
  {
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
  }

  @Nonnull
  public static String getChannelId(String workerTaskId, String path)
  {
    return StringUtils.format("%s:%s", workerTaskId, path);
  }

  @Nonnull
  public static String getStagePartitionPath(StageId stageId, int partitionNumber)
  {
    return StringUtils.format(
        "/channels/%s/%d/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        partitionNumber
    );
  }

  @Override
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    return Futures.transform(
        getClient(workerTaskId).asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/workOrder")
                .content(MediaType.APPLICATION_JSON, jsonMapper, workOrder),
            IgnoreHttpResponseHandler.INSTANCE
        ),
        (Function<Either<RpcServerError, Void>, Void>) Either::valueOrThrow
    );
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  )
  {
    final String path = StringUtils.format(
        "/resultPartitionBoundaries/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return Futures.transform(
        getClient(workerTaskId).asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .content(MediaType.APPLICATION_JSON, jsonMapper, partitionBoundaries),
            IgnoreHttpResponseHandler.INSTANCE
        ),
        (Function<Either<RpcServerError, Void>, Void>) Either::valueOrThrow
    );
  }

  /**
   * Client-side method for {@link WorkerChatHandler#httpPostCleanupStage}.
   */
  @Override
  public ListenableFuture<Void> postCleanupStage(
      final String workerTaskId,
      final StageId stageId
  )
  {
    final String path = StringUtils.format(
        "/cleanupStage/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return Futures.transform(
        getClient(workerTaskId).asyncRequest(
            new RequestBuilder(HttpMethod.POST, path),
            IgnoreHttpResponseHandler.INSTANCE
        ),
        (Function<Either<RpcServerError, Void>, Void>) Either::valueOrThrow
    );
  }

  @Override
  public ListenableFuture<Void> postFinish(String workerTaskId)
  {
    return Futures.transform(
        getClient(workerTaskId).asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/finish"),
            IgnoreHttpResponseHandler.INSTANCE
        ),
        (Function<Either<RpcServerError, Void>, Void>) Either::valueOrThrow
    );
  }

  @Override
  public ListenableFuture<TalariaCountersSnapshot> getCounters(String workerTaskId)
  {
    return Futures.transform(
        getClient(workerTaskId).asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/counters"),
            JsonHttpResponseHandler.create(jsonMapper, TalariaCountersSnapshot.class)
        ),
        (Function<Either<RpcServerError, TalariaCountersSnapshot>, TalariaCountersSnapshot>) Either::valueOrThrow
    );
  }

  @Override
  public ReadableFrameChannel getChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      ExecutorService connectExec
  )
  {
    final DruidServiceClient client = getClient(workerTaskId);

    final String path = getStagePartitionPath(stageId, partitionNumber);
    final String channelId = getChannelId(workerTaskId, path);
    final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create(channelId);
    final TalariaFrameChannelConnectionManager connectionManager =
        new TalariaFrameChannelConnectionManager(channel, connectExec);

    return connectionManager.connect(
        (connectionNumber, offset) ->
            client.request(
                // Include read timeout even though these calls may take a long time. If something has gone wrong with
                // the connection, disconnecting and allowing the connection manager to reconnect can jog it back to
                // a working state. Use a longer timeout than the standard one, though.
                new RequestBuilder(HttpMethod.GET, StringUtils.format("%s?offset=%d", path, offset))
                    .header(HttpHeaders.ACCEPT_ENCODING, "identity") // Data is compressed at app level
                    .timeout(HTTP_CHANNEL_TIMEOUT),
                new FrameFileHttpResponseHandler(
                    channel,
                    e -> connectionManager.handleChannelException(connectionNumber, e)
                )
            )
    );
  }

  @Override
  public void close() throws IOException
  {
    synchronized (clientMap) {
      try {
        final List<Closeable> closeables =
            clientMap.values().stream().map(pair -> pair.rhs).collect(Collectors.toList());
        CloseableUtils.closeAll(closeables);
      }
      finally {
        clientMap.clear();
      }
    }
  }

  private DruidServiceClient getClient(final String workerTaskId)
  {
    synchronized (clientMap) {
      return clientMap.computeIfAbsent(
          workerTaskId,
          id -> {
            final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(id, overlordClient);
            final DruidServiceClient client = clientFactory.makeClient(
                id,
                locator,
                new SpecificTaskRetryPolicy(workerTaskId)
            );
            return Pair.of(client, locator);
          }
      ).lhs;
    }
  }
}
