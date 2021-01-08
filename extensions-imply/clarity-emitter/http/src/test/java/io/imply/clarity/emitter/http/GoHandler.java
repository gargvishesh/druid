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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class GoHandler
{
  /******* Abstract Methods *********/
  protected abstract <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration requestReadTimeout
  ) throws Exception;

  /******* Non Abstract Methods ********/
  private volatile boolean succeeded = false;

  public boolean succeeded()
  {
    return succeeded;
  }

  public <Intermediate, Final> ListenableFuture<Final> run(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler
  ) throws Exception
  {
    return run(request, handler, null);
  }

  public <Intermediate, Final> ListenableFuture<Final> run(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration requestReadTimeout
  ) throws Exception
  {
    try {
      final ListenableFuture<Final> retVal = go(request, handler, requestReadTimeout);
      succeeded = true;
      return retVal;
    }
    catch (Throwable e) {
      succeeded = false;
      Throwables.propagateIfPossible(e, Exception.class);
      throw Throwables.propagate(e);
    }
  }

  public GoHandler times(final int n)
  {
    final GoHandler myself = this;

    return new GoHandler()
    {
      AtomicInteger counter = new AtomicInteger(0);

      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(
          final Request request,
          final HttpResponseHandler<Intermediate, Final> handler,
          final Duration requestReadTimeout
      ) throws Exception
      {
        if (counter.getAndIncrement() < n) {
          return myself.go(request, handler, requestReadTimeout);
        }
        succeeded = false;
        throw new ISE("Called more than %d times", n);
      }
    };
  }
}
