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

import java.io.IOException;

/**
 * Represents a channel for producers to write to. It can abstract over the transformations, computations and the way
 * these individual frames are stored physically in memory or disk. This doesn't provide a way of extracting the data from
 * the written to channel, the users of this interface are required to know the specific implementation of the same
 * and extract the data approperiately
 * This is meant for the usage of a single producer only
 */
public interface WritableFrameChannel
{
  default void write(FrameWithPartition frame) throws IOException
  {
    write(Try.value(frame));
  }

  /**
   * Writes to the frame channel. If the channel is full, then the current implementations of the interface can throw
   * an error but may choose not to.
   */
  void write(Try<FrameWithPartition> frameTry) throws IOException;

  void doneWriting() throws IOException;

  ListenableFuture<?> writabilityFuture();
}
