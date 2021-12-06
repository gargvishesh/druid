/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper used by {@link FrameProcessorExecutor#runFully} when workers return {@link ReturnOrAwait#awaitAny}.
 *
 * The main idea is to reuse listeners from previous calls to {@link #awaitAny(IntSet)} in cases where a particular
 * channel has not receieved any input since the last call. (Otherwise, listeners would pile up.)
 */
public class AwaitAnyWidget
{
  private final List<ReadableFrameChannel> channels;

  @GuardedBy("listeners")
  private final List<Listener> listeners;

  public AwaitAnyWidget(final List<ReadableFrameChannel> channels)
  {
    this.channels = channels;
    this.listeners = new ArrayList<>(channels.size());

    for (int i = 0; i < channels.size(); i++) {
      listeners.add(null);
    }
  }

  public ListenableFuture<?> awaitAny(final IntSet awaitSet)
  {
    synchronized (listeners) {
      final SettableFuture<?> retVal = SettableFuture.create();

      final IntIterator awaitSetIterator = awaitSet.iterator();
      while (awaitSetIterator.hasNext()) {
        final int channelNumber = awaitSetIterator.nextInt();
        final ReadableFrameChannel channel = channels.get(channelNumber);

        if (channel.canRead() || channel.isFinished()) {
          retVal.set(null);
          return retVal;
        } else {
          final Listener priorListener = listeners.get(channelNumber);
          if (priorListener == null || !priorListener.replaceFuture(retVal)) {
            final Listener newListener = new Listener(retVal);
            channel.readabilityFuture().addListener(newListener, Execs.directExecutor());
            listeners.set(channelNumber, newListener);
          }
        }
      }

      return retVal;
    }
  }

  private static class Listener implements Runnable
  {
    @GuardedBy("this")
    private SettableFuture<?> future;

    public Listener(SettableFuture<?> future)
    {
      this.future = future;
    }

    @Override
    public void run()
    {
      synchronized (this) {
        try {
          future.set(null);
        }
        finally {
          future = null;
        }
      }
    }

    /**
     * Replaces our future if the listener has not fired yet.
     *
     * Returns true if the future was replaced, false if the listener has already fired.
     */
    public boolean replaceFuture(final SettableFuture<?> newFuture)
    {
      synchronized (this) {
        if (future == null) {
          return false;
        } else {
          future = newFuture;
          return true;
        }
      }
    }
  }
}
