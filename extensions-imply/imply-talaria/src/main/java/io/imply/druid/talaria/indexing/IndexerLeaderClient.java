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
import io.imply.druid.talaria.exec.LeaderClient;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.rpc.DruidServiceClient;
import io.imply.druid.talaria.rpc.RequestBuilder;
import io.imply.druid.talaria.rpc.handler.IgnoreHttpResponseHandler;
import io.imply.druid.talaria.rpc.handler.JsonHttpResponseHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class IndexerLeaderClient implements LeaderClient
{
  private final DruidServiceClient serviceClient;
  private final ObjectMapper jsonMapper;
  private final Closeable baggage;

  public IndexerLeaderClient(
      final DruidServiceClient serviceClient,
      final ObjectMapper jsonMapper,
      final Closeable baggage
  )
  {
    this.serviceClient = serviceClient;
    this.jsonMapper = jsonMapper;
    this.baggage = baggage;
  }

  @Override
  public void postKeyStatistics(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot keyStatistics
  )
  {
    final String path = StringUtils.format(
        "/keyStatistics/%s/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        workerNumber
    );

    serviceClient.request(
        new RequestBuilder(HttpMethod.POST, path)
            .content(MediaType.APPLICATION_JSON, jsonMapper, keyStatistics),
        IgnoreHttpResponseHandler.INSTANCE
    ).valueOrThrow();
  }

  @Override
  public void postCounters(String workerId, MSQCountersSnapshot.WorkerCounters snapshot)
  {
    serviceClient.request(
        new RequestBuilder(HttpMethod.POST, StringUtils.format("/counters/%s", StringUtils.urlEncode(workerId)))
            .content(MediaType.APPLICATION_JSON, jsonMapper, snapshot),
        IgnoreHttpResponseHandler.INSTANCE
    ).valueOrThrow();
  }

  @Override
  public void postResultsComplete(StageId stageId, int workerNumber, @Nullable Object resultObject)
  {
    final String path = StringUtils.format(
        "/resultsComplete/%s/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        workerNumber
    );

    serviceClient.request(
        new RequestBuilder(HttpMethod.POST, path)
            .content(MediaType.APPLICATION_JSON, jsonMapper, resultObject),
        IgnoreHttpResponseHandler.INSTANCE
    ).valueOrThrow();
  }

  @Override
  public void postWorkerError(String workerId, MSQErrorReport errorWrapper)
  {
    final String path = StringUtils.format(
        "/workerError/%s",
        StringUtils.urlEncode(workerId)
    );

    serviceClient.request(
        new RequestBuilder(HttpMethod.POST, path)
            .content(MediaType.APPLICATION_JSON, jsonMapper, errorWrapper),
        IgnoreHttpResponseHandler.INSTANCE
    ).valueOrThrow();
  }

  @Override
  public void postWorkerWarning(String workerId, List<MSQErrorReport> MSQErrorReports)
  {
    final String path = StringUtils.format(
        "/workerWarning/%s",
        StringUtils.urlEncode(workerId)
    );

    serviceClient.request(
        new RequestBuilder(HttpMethod.POST, path)
            .content(MediaType.APPLICATION_JSON, jsonMapper, MSQErrorReports),
        IgnoreHttpResponseHandler.INSTANCE
    ).valueOrThrow();
  }

  @Override
  public Optional<List<String>> getTaskList()
  {
    final MSQTaskList retVal = serviceClient.request(
        new RequestBuilder(HttpMethod.GET, "/taskList"),
        JsonHttpResponseHandler.create(jsonMapper, MSQTaskList.class)
    ).valueOrThrow();

    return Optional.ofNullable(retVal.getTaskIds());
  }

  @Override
  public void close()
  {
    try {
      baggage.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
