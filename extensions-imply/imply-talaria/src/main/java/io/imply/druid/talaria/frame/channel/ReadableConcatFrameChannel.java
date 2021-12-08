/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.read.Frame;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Channel that concatenates a sequence of other channels that are provided by an iterator. The iterator is
 * walked just-in-time, meaning that {@link Iterator#next()} is not called until the channel is actually ready
 * to be used.
 *
 * The first channel is pulled from the iterator immediately upon construction.
 */
public class ReadableConcatFrameChannel implements ReadableFrameChannel
{
  private final Iterator<ReadableFrameChannel> channelIterator;

  // Null means there were never any channels to begin with.
  @Nullable
  private ReadableFrameChannel currentChannel;

  private ReadableConcatFrameChannel(Iterator<ReadableFrameChannel> channelIterator)
  {
    this.channelIterator = channelIterator;
    this.currentChannel = channelIterator.hasNext() ? channelIterator.next() : null;
  }

  public static ReadableConcatFrameChannel open(final Iterator<ReadableFrameChannel> channelIterator)
  {
    return new ReadableConcatFrameChannel(channelIterator);
  }

  @Override
  public boolean isFinished()
  {
    advanceCurrentChannelIfFinished();
    return currentChannel == null || currentChannel.isFinished();
  }

  @Override
  public boolean canRead()
  {
    advanceCurrentChannelIfFinished();
    return currentChannel != null && currentChannel.canRead();
  }

  @Override
  public Try<Frame> read()
  {
    if (!canRead() || currentChannel == null) {
      throw new NoSuchElementException();
    }

    return currentChannel.read();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    advanceCurrentChannelIfFinished();

    if (currentChannel == null) {
      return Futures.immediateFuture(null);
    } else {
      return currentChannel.readabilityFuture();
    }
  }

  @Override
  public void doneReading()
  {
    if (currentChannel != null) {
      currentChannel.doneReading();
    }
  }

  private void advanceCurrentChannelIfFinished()
  {
    while (currentChannel != null && currentChannel.isFinished() && channelIterator.hasNext()) {
      currentChannel = channelIterator.next();
    }
  }
}
