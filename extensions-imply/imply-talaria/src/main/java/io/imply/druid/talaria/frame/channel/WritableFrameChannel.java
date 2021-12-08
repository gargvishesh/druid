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
 * TODO(gianm): Javadocs. meant to be used by a single producer and single consumer.
 */
public interface WritableFrameChannel
{
  default void write(FrameWithPartition frame) throws IOException
  {
    write(Try.value(frame));
  }

  /**
   * TODO(gianm): spec behavior for what happens if the channel is full
   * TODO(gianm): make it so in-memory channels can always accept an error (by overwriting unread stuff?)
   */
  void write(Try<FrameWithPartition> frameTry) throws IOException;

  void doneWriting() throws IOException;

  ListenableFuture<?> writabilityFuture();
}
