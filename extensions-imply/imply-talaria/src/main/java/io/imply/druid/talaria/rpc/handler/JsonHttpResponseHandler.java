/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Response handler that deserializes responses as JSON.
 *
 * This class does not look at the HTTP code; it tries to deserialize any response with the proper content type. If
 * you don't want this, consider wrapping the handler in
 * {@link org.apache.druid.java.util.http.client.response.ObjectOrErrorResponseHandler} so it will only be called
 * for 2xx responses. Alternatively, consider using this handler through
 * {@link io.imply.druid.talaria.rpc.DruidServiceClient#asyncRequest} or
 * {@link io.imply.druid.talaria.rpc.DruidServiceClient#request}, which only call provided handlers for 2xx responses.
 */
public class JsonHttpResponseHandler<T>
    implements HttpResponseHandler<BytesFullResponseHolder, T>
{
  private static final String CONTENT_TYPE_HEADER = "content-type";

  private final DeserializeFn<T> deserializeFn;
  private final BytesFullResponseHandler delegate;

  private JsonHttpResponseHandler(final DeserializeFn<T> deserializeFn)
  {
    this.deserializeFn = deserializeFn;
    this.delegate = new BytesFullResponseHandler();
  }

  public static <T> JsonHttpResponseHandler<T> create(final ObjectMapper jsonMapper, final Class<T> clazz)
  {
    return new JsonHttpResponseHandler<>(in -> jsonMapper.readValue(in, clazz));
  }

  public static <T> JsonHttpResponseHandler<T> create(final ObjectMapper jsonMapper, final TypeReference<T> typeRef)
  {
    return new JsonHttpResponseHandler<>(in -> jsonMapper.readValue(in, typeRef));
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> handleResponse(
      final HttpResponse response,
      final TrafficCop trafficCop
  )
  {
    return delegate.handleResponse(response, trafficCop);
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> handleChunk(
      final ClientResponse<BytesFullResponseHolder> clientResponse,
      final HttpChunk chunk,
      final long chunkNum
  )
  {
    return delegate.handleChunk(clientResponse, chunk, chunkNum);
  }

  @Override
  public ClientResponse<T> done(final ClientResponse<BytesFullResponseHolder> clientResponse)
  {
    final ClientResponse<BytesFullResponseHolder> delegateResponse = delegate.done(clientResponse);
    final String encoding = delegateResponse.getObj().getResponse().headers().get(CONTENT_TYPE_HEADER);

    if (MediaType.APPLICATION_JSON.equals(encoding)) {
      try {
        return ClientResponse.finished(deserializeFn.deserialize(delegateResponse.getObj().getContent()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("Response was not JSON-encoded");
    }
  }

  @Override
  public void exceptionCaught(final ClientResponse<BytesFullResponseHolder> clientResponse, final Throwable e)
  {
    delegate.exceptionCaught(clientResponse, e);
  }

  private interface DeserializeFn<T>
  {
    T deserialize(byte[] in) throws IOException;
  }
}
