/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

/**
 */
public class MockHttpClient implements HttpClient
{
  private volatile GoHandler goHandler;

  public void setGoHandler(GoHandler goHandler)
  {
    this.goHandler = goHandler;
  }

  public boolean succeeded()
  {
    return goHandler.succeeded();
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    return go(request, httpResponseHandler, Duration.ZERO);
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration requestReadTimeout
  )
  {
    try {
      return goHandler.run(request, handler, requestReadTimeout);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
