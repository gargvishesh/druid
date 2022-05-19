/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;

import java.util.concurrent.ExecutionException;

/**
 * Mid-level client that provides an API similar to low-level {@link HttpClient}, but accepts {@link RequestBuilder}
 * instead of {@link org.apache.druid.java.util.http.client.Request}, and internally handles service location
 * and retries.
 *
 * In most cases, this client is further wrapped in a high-level client like
 * {@link io.imply.druid.talaria.rpc.indexing.OverlordServiceClient} or
 * {@link io.imply.druid.talaria.indexing.IndexerWorkerClient}.
 */
public interface DruidServiceClient
{
  long MAX_REDIRECTS = 3;

  /**
   * Perform a request asynchronously.
   *
   * Unlike {@link HttpClient#go}, the provided "handler" is only be used for 2xx responses.
   *
   * Redirects from 3xx responses are followed up to {@link #MAX_REDIRECTS} and do not consume attempts. However,
   * redirects are only followed if the target is returned by the {@link ServiceLocator}. Redirects to other
   * targets are ignored.
   *
   * Response codes 1xx, 4xx, and 5xx are retried with backoff according to this client's {@link RetryPolicy}. If
   * attempts are exhausted, the future will resolve to {@link RpcServerError} containing the most recent server error.
   *
   * If the service is unavailable -- i.e. if {@link ServiceLocator#locate()} returns an empty set -- an attempt is
   * consumed and the client will try to locate the service again, with backoff.
   *
   * If an exception occurs without a corresponding server response -- for example, a socket exception -- then the
   * returned future will throw an exception on {@link ListenableFuture#get()}.
   */
  <IntermediateType, FinalType> ListenableFuture<Either<RpcServerError, FinalType>> asyncRequest(
      RequestBuilder requestBuilder,
      HttpResponseHandler<IntermediateType, FinalType> handler
  );

  /**
   * Perform a request synchronously.
   *
   * Same behavior as {@link #asyncRequest}, except the result is returned synchronously.
   */
  default <IntermediateType, FinalType> Either<RpcServerError, FinalType> request(
      RequestBuilder requestBuilder,
      HttpResponseHandler<IntermediateType, FinalType> handler
  )
  {
    final ListenableFuture<Either<RpcServerError, FinalType>> future = asyncRequest(requestBuilder, handler);

    try {
      return future.get();
    }
    catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    catch (ExecutionException e) {
      // Unwrap and convert to unchecked exception.
      throw new RuntimeException(e.getCause());
    }
  }

  /**
   * Returns a copy of this client with a different {@link RetryPolicy}.
   */
  DruidServiceClient withRetryPolicy(RetryPolicy retryPolicy);
}
