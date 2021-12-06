/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.read.Frame;

/**
 * TODO(gianm): Needs a way to detect that the thing writing to the channel has died (?) Or maybe not, if we can use whole-system cancellation
 */
public interface ReadableFrameChannel
{
  /**
   * Returns whether this channel is finished. Finished channels will not generate any further frames or errors.
   *
   * Generally, once you discover that a channel is finished, you should call {@link #doneReading()} and then
   * discard it.
   *
   * Note that it is possible for a channel to be unfinished and also have no available frames or errors. This happens
   * when it is not in a ready-for-reading state. See {@link #readabilityFuture()} for details.
   */
  boolean isFinished();

  /**
   * Returns whether this channel has a frame or error condition currently available. If this method returns true, then
   * you can call {@link #read()} to retrieve the frame or error.
   *
   * Note that it is possible for a channel to be unfinished and also have no available frames or errors. This happens
   * when it is not in a ready-for-reading state. See {@link #readabilityFuture()} for details.
   */
  boolean canRead();

  /**
   * Returns the next available frame or error from this channel.
   *
   * Note that the Try construct is only used to return errors that were *sent into this channel* by the upstream
   * writer. Errors that occur while reading from the channel will be thrown as normal exceptions.
   *
   * Before calling this method, you should check {@link #canRead()} to ensure there is a frame or
   * error available.
   *
   * @throws java.util.NoSuchElementException if there is no frame or error currently available
   */
  Try<Frame> read();

  /**
   * Returns a future that will resolve when either {@link #isFinished()} or {@link #canRead()} would
   * return true. The future will never resolve to an exception. If something exceptional has happened, the exception
   * can be retrieved from {@link #read()}.
   */
  ListenableFuture<?> readabilityFuture();

  /**
   * Releases any resources associated with this readable channel. After calling this, you should not call any other
   * methods on the channel.
   *
   * TODO(gianm): gracefully handle early-closing (like if there is a limiting worker that doesn't read all inputs)
   * TODO(gianm): many implementations do not handle this very well
   * TODO(gianm): especially make sure this works with FrameFileHttpResponseHandler
   */
  void doneReading();
}
