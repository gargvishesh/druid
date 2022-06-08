/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import io.imply.druid.talaria.frame.Frame;
import org.apache.druid.java.util.common.guava.BaseSequence;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

public class FrameChannelSequence extends BaseSequence<Frame, FrameChannelSequence.FrameChannelIterator>
{
  public FrameChannelSequence(final ReadableFrameChannel channel)
  {
    super(
        new IteratorMaker<Frame, FrameChannelIterator>()
        {
          @Override
          public FrameChannelIterator make()
          {
            return new FrameChannelIterator(channel);
          }

          @Override
          public void cleanup(FrameChannelIterator iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  public static class FrameChannelIterator implements Iterator<Frame>, Closeable
  {
    private final ReadableFrameChannel channel;

    private FrameChannelIterator(final ReadableFrameChannel channel)
    {
      this.channel = channel;
    }

    @Override
    public boolean hasNext()
    {
      await();
      return !channel.isFinished();
    }

    @Override
    public Frame next()
    {
      await();

      if (!channel.canRead()) {
        throw new NoSuchElementException();
      }

      return channel.read().getOrThrow();
    }

    @Override
    public void close()
    {
      channel.doneReading();
    }

    private void await()
    {
      try {
        channel.readabilityFuture().get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
