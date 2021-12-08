/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TalariaFrameChannelConnectionManager
{
  private static final Logger log = new Logger(TalariaFrameChannelConnectionManager.class);

  // TODO(gianm): no
  private static final int MAX_CONNECTION_ATTEMPTS = 10000;

  private final String id;
  private final ReadableByteChunksFrameChannel channel;
  private final ExecutorService connectExec;
  private final AtomicInteger connectionCount = new AtomicInteger(0);
  private final AtomicReference<ConnectFunction> connectFunction = new AtomicReference<>();

  public TalariaFrameChannelConnectionManager(
      String id,
      ReadableByteChunksFrameChannel channel,
      ExecutorService connectExec
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.channel = Preconditions.checkNotNull(channel, "channel");
    this.connectExec = Preconditions.checkNotNull(connectExec, "connectExec");
  }

  public ReadableFrameChannel connect(final ConnectFunction connectFunction)
  {
    if (!this.connectFunction.compareAndSet(null, connectFunction)) {
      throw new ISE("Cannot call setConnectRunnable more than once");
    }

    connectIfPossible(0, 0);
    return channel;
  }

  public void handleChannelException(final int connectionNumber, final Throwable e)
  {
    // TODO(gianm): don't retry every kind of exception
    // TODO(gianm): smarter retry limiter other than fixed-count
    final int newConnectionNumber = connectionNumber + 1;

    if (newConnectionNumber >= MAX_CONNECTION_ATTEMPTS) {
      channel.setError(new RE(e, "Maximum number of retries [%d] exhausted", MAX_CONNECTION_ATTEMPTS));
    } else if (!channel.isErrorOrFinished() && connectionCount.compareAndSet(connectionNumber, newConnectionNumber)) {
      log.debug(
          "Reconnecting channel for [%s], attempt %d/%d (offset = %d)",
          id,
          newConnectionNumber,
          MAX_CONNECTION_ATTEMPTS,
          channel.getBytesAdded()
      );

      connectIfPossible(newConnectionNumber, channel.getBytesAdded());
    }
  }

  /**
   * Connect our channel to a new HTTP connection if it is not in an error or finished state. Only call this method
   * if the channel is not currently connected.
   *
   * If the channel is in an error/finished state, this method does nothing. If the channel is already connected, this
   * method throws an error, because that is an indication that it is being used improperly.
   */
  private void connectIfPossible(final int connectionNumber, final long offset)
  {
    connectExec.submit(() -> {
      try {
        connectFunction.get().connect(connectionNumber, offset);
      }
      catch (Throwable e) {
        channel.setError(e);
      }
    });
  }

  public interface ConnectFunction
  {
    void connect(int connectionNumber, long offset) throws IOException;
  }
}
