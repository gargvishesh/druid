/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.rpc.DruidServiceClient;
import io.imply.druid.talaria.rpc.RequestBuilder;
import io.imply.druid.talaria.rpc.RetryPolicy;
import io.imply.druid.talaria.rpc.RpcServerError;
import io.imply.druid.talaria.rpc.handler.IgnoreHttpResponseHandler;
import io.imply.druid.talaria.rpc.handler.JsonHttpResponseHandler;
import io.imply.druid.talaria.util.FutureUtils;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Set;

/**
 * Production implementation of {@link OverlordServiceClient}.
 */
public class OverlordServiceClientImpl implements OverlordServiceClient
{
  private final DruidServiceClient client;
  private final ObjectMapper jsonMapper;

  public OverlordServiceClientImpl(final DruidServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = Preconditions.checkNotNull(client, "client");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
  }

  @Override
  public ListenableFuture<Void> runTask(final String taskId, final Object taskObject)
  {
    final ListenableFuture<Either<RpcServerError, Map<String, Object>>> response = client.asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/task")
            .content(MediaType.APPLICATION_JSON, jsonMapper, taskObject),
        JsonHttpResponseHandler.create(jsonMapper, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)
    );

    return Futures.transform(
        response,
        (Function<Either<RpcServerError, Map<String, Object>>, Void>) input -> {
          final String returnedTaskId = (String) input.valueOrThrow().get("task");

          Preconditions.checkState(
              taskId.equals(returnedTaskId),
              "Got a different taskId[%s]. Expected taskId[%s]",
              returnedTaskId,
              taskId
          );

          return null;
        }
    );
  }

  @Override
  public ListenableFuture<Void> cancelTask(final String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s/shutdown", StringUtils.urlEncode(taskId));

    final ListenableFuture<Either<RpcServerError, Void>> response =
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path),
            IgnoreHttpResponseHandler.INSTANCE
        );

    return FutureUtils.transform(response, Either::valueOrThrow);
  }

  @Override
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(final Set<String> taskIds)
  {
    final ListenableFuture<Either<RpcServerError, Map<String, TaskStatus>>> response =
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/taskStatus")
                .content(MediaType.APPLICATION_JSON, jsonMapper, taskIds),
            JsonHttpResponseHandler.create(jsonMapper, new TypeReference<Map<String, TaskStatus>>() {})
        );

    return FutureUtils.transform(response, Either::valueOrThrow);
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(final String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s/status", StringUtils.urlEncode(taskId));

    final ListenableFuture<Either<RpcServerError, TaskStatusResponse>> response =
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            JsonHttpResponseHandler.create(jsonMapper, TaskStatusResponse.class)
        );

    return FutureUtils.transform(response, Either::valueOrThrow);
  }

  @Override
  public OverlordServiceClientImpl withRetryPolicy(RetryPolicy retryPolicy)
  {
    return new OverlordServiceClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }
}
