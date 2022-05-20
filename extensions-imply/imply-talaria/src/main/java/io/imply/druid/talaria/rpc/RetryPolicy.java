/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * Used by {@link DruidServiceClient} to decide whether to retry requests.
 */
public interface RetryPolicy
{
  int UNLIMITED = -1;

  /**
   * Returns the maximum number of desired attempts, or {@link #UNLIMITED} if unlimited. A value of 1 means no retries.
   * Zero is invalid.
   */
  int maxAttempts();

  /**
   * Returns whether the given HTTP response can be retried. The response will have a non-2xx error code.
   */
  boolean retryHttpResponse(HttpResponse response);

  /**
   * Returns whether the given exception can be retried.
   */
  boolean retryThrowable(Throwable t);
}
