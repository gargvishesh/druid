/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc.handler;

import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * An HTTP response handler that discards the response and returns nothing. It returns a finished response only
 * when the entire HTTP response is done.
 */
public class IgnoreHttpResponseHandler implements HttpResponseHandler<Void, Void>
{
  public static final IgnoreHttpResponseHandler INSTANCE = new IgnoreHttpResponseHandler();

  private IgnoreHttpResponseHandler()
  {
    // Singleton.
  }

  @Override
  public ClientResponse<Void> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    return ClientResponse.unfinished(null);
  }

  @Override
  public ClientResponse<Void> handleChunk(ClientResponse<Void> clientResponse, HttpChunk chunk, long chunkNum)
  {
    return ClientResponse.unfinished(null);
  }

  @Override
  public ClientResponse<Void> done(ClientResponse<Void> clientResponse)
  {
    return ClientResponse.finished(null);
  }

  @Override
  public void exceptionCaught(ClientResponse<Void> clientResponse, Throwable e)
  {
    // Safe to ignore, since the ClientResponses returned in handleChunk were unfinished.
  }
}
