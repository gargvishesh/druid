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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * An {@link HttpResponseHandler} that streams data into a {@link ReadableByteChunksFrameChannel}.
 *
 * In the successful case, this class calls {@link ReadableByteChunksFrameChannel#doneWriting()} on the channel.
 * However, in the unsuccessful case, this class does *not* set an error on the channel. Instead, it calls the
 * provided {@link #exceptionCallback}. This allows for reconnection and resuming, which is typically managed
 * by a {@link io.imply.druid.talaria.indexing.TalariaFrameChannelConnectionManager}.
 *
 * This class implements backpressure: when {@link ReadableByteChunksFrameChannel#addChunk} returns a backpressure
 * future, we use the {@link HttpResponseHandler.TrafficCop} mechanism to throttle reading.
 */
public class FrameFileHttpResponseHandler
    implements HttpResponseHandler<ReadableByteChunksFrameChannel, ReadableFrameChannel>
{
  private final ReadableByteChunksFrameChannel channel;
  private final Consumer<Throwable> exceptionCallback;
  private final AtomicBoolean exceptionCaught = new AtomicBoolean();

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
      // Note: if the error body is chunked, we will discard all future chunks due to setting exceptionCaught here.
      // This is OK because we don't need the body; just the HTTP status code.
      handleException(new ISE("Server for [%s] returned [%s]", channel.getId(), response.getStatus()));
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

    if (!exceptionCaught.get()) {
      // If there was an exception, don't close the channel. The exception callback will do this if it wants to.
      channel.doneWriting();
    }

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
    if (exceptionCaught.get()) {
      // If there was an exception, exit early without updating "channel". Important because "handleChunk" can be
      // called after "handleException" in two cases: it can be called after an error "response", if the error body
      // is chunked; and it can be called when "handleChunk" is called after "exceptionCaught". In neither case do
      // we want to add that extra chunk to the channel.
      return ClientResponse.finished(channel, true);
    }

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
    if (exceptionCaught.compareAndSet(false, true)) {
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
}
