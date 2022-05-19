/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import org.apache.druid.java.util.common.IAE;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;

public class StandardRetryPolicy implements RetryPolicy
{
  private static final StandardRetryPolicy UNLIMITED_POLICY = new StandardRetryPolicy(UNLIMITED);
  private static final StandardRetryPolicy NO_RETRIES_POLICY = new StandardRetryPolicy(1);

  private final int maxAttempts;

  public StandardRetryPolicy(final int maxAttempts)
  {
    this.maxAttempts = maxAttempts;

    if (maxAttempts == 0) {
      throw new IAE("maxAttempts must be positive (limited) or negative (unlimited); cannot be zero.");
    }
  }

  public static StandardRetryPolicy unlimited()
  {
    return UNLIMITED_POLICY;
  }

  public static StandardRetryPolicy noRetries()
  {
    return NO_RETRIES_POLICY;
  }

  @Override
  public int maxAttempts()
  {
    return maxAttempts;
  }

  @Override
  public boolean retryHttpResponse(final HttpResponse response)
  {
    final int code = response.getStatus().getCode();

    return code == HttpResponseStatus.BAD_GATEWAY.getCode()
           || code == HttpResponseStatus.SERVICE_UNAVAILABLE.getCode()
           || code == HttpResponseStatus.GATEWAY_TIMEOUT.getCode()

           // Technically shouldn't retry this last one, but servers sometimes return HTTP 500 for retryable errors.
           || code == HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode();
  }

  @Override
  public boolean retryThrowable(Throwable t)
  {
    return t instanceof IOException
           || t instanceof ChannelException
           || (t.getCause() != null && retryThrowable(t.getCause()));
  }
}
