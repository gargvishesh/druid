/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

/**
 */
public class GoHandlers
{
  public static GoHandler passingHandler(final Object retVal)
  {
    return new GoHandler()
    {
      @SuppressWarnings("unchecked")
      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(
          Request request,
          HttpResponseHandler<Intermediate, Final> handler,
          Duration requestReadTimeout
      )
      {
        return Futures.immediateFuture((Final) retVal);
      }
    };
  }
}
