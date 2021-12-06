/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Optional;
import java.util.function.Consumer;

public class FrameFileHttpResponseHandler
    implements HttpResponseHandler<ReadableByteChunksFrameChannel, ReadableFrameChannel>
{
  private final ReadableByteChunksFrameChannel channel;
  private final Consumer<Throwable> exceptionCallback;

  private volatile TrafficCop trafficCop;

  public FrameFileHttpResponseHandler(
      final ReadableByteChunksFrameChannel channel,
      final Consumer<Throwable> exceptionCallback
  )
  {
    this.channel = Preconditions.checkNotNull(channel, "channel");
    this.exceptionCallback = Preconditions.checkNotNull(exceptionCallback, "exceptionCallback");
  }

  @Override
  public ClientResponse<ReadableByteChunksFrameChannel> handleResponse(
      final HttpResponse response,
      final TrafficCop trafficCop
  )
  {
    this.trafficCop = trafficCop;

    if (response.getStatus().getCode() != HttpResponseStatus.OK.getCode()) {
      // TODO(gianm): Nicer error. Use response content? ... Handle chunked error responses?
      handleException(new ISE("not ok, status = %s", response.getStatus()));
      return ClientResponse.finished(channel, true);
    } else {
      return response(channel, response.getContent(), 0);
    }
  }

  @Override
  public ClientResponse<ReadableByteChunksFrameChannel> handleChunk(
      final ClientResponse<ReadableByteChunksFrameChannel> clientResponse,
      final HttpChunk chunk,
      final long chunkNum
  )
  {
    return response(clientResponse.getObj(), chunk.getContent(), chunkNum);
  }

  @Override
  public ClientResponse<ReadableFrameChannel> done(final ClientResponse<ReadableByteChunksFrameChannel> clientResponse)
  {
    final ReadableByteChunksFrameChannel channel = clientResponse.getObj();
    channel.doneWriting();
    return ClientResponse.finished(channel);
  }

  @Override
  public void exceptionCaught(final ClientResponse<ReadableByteChunksFrameChannel> clientResponse, final Throwable e)
  {
    handleException(e);
  }

  private ClientResponse<ReadableByteChunksFrameChannel> response(
      final ReadableByteChunksFrameChannel channel,
      final ChannelBuffer content,
      final long chunkNum
  )
  {
    final byte[] chunk = new byte[content.readableBytes()];
    content.getBytes(content.readerIndex(), chunk);
    final Optional<ListenableFuture<?>> addVal = channel.addChunk(chunk);

    final boolean keepReading;

    if (!addVal.isPresent()) {
      keepReading = true;
    } else {
      keepReading = false;
      addVal.get().addListener(
          () -> trafficCop.resume(chunkNum),
          Execs.directExecutor()
      );
    }

    return ClientResponse.finished(channel, keepReading);
  }

  private void handleException(final Throwable e)
  {
    try {
      exceptionCallback.accept(e);
    }
    catch (Throwable e2) {
      e2.addSuppressed(e);

      if (channel != null) {
        channel.setError(e2);
      }
    }
  }
}
