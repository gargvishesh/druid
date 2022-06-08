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
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.Frame;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * In-memory channel backed by a limited-capacity {@link java.util.Deque}.
 */
public class BlockingQueueFrameChannel implements WritableFrameChannel, ReadableFrameChannel
{
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static final Optional<Try<FrameWithPartition>> END_MARKER = Optional.empty();

  private final int maxQueuedFrames;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private final ArrayDeque<Optional<Try<FrameWithPartition>>> queue;

  @GuardedBy("lock")
  private SettableFuture<?> readyForWritingFuture = null;

  @GuardedBy("lock")
  private SettableFuture<?> readyForReadingFuture = null;

  public BlockingQueueFrameChannel(final int maxQueuedFrames)
  {
    if (maxQueuedFrames < 1 || maxQueuedFrames == Integer.MAX_VALUE) {
      throw new IAE("Cannot handle capacity of [%d]", maxQueuedFrames);
    }

    this.maxQueuedFrames = maxQueuedFrames;

    // Plus one to leave space for END_MARKER.
    this.queue = new ArrayDeque<>(maxQueuedFrames + 1);
  }

  public static BlockingQueueFrameChannel minimal()
  {
    return new BlockingQueueFrameChannel(1);
  }

  @Override
  public void write(Try<FrameWithPartition> frameTry)
  {
    synchronized (lock) {
      if (isFinished()) {
        throw new ISE("Channel cannot accept new frames");
      } else if (queue.size() >= maxQueuedFrames) {
        // Caller should have checked if this channel was ready for writing.
        throw new ISE("Channel has no capacity");
      } else {
        if (!queue.offer(Optional.of(frameTry))) {
          // If this happens, it's a bug in this class's capacity-counting.
          throw new ISE("Channel had capacity, but could not add frame");
        }
      }

      notifyReader();
    }
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    synchronized (lock) {
      if (queue.size() < maxQueuedFrames) {
        return Futures.immediateFuture(null);
      } else if (readyForWritingFuture != null) {
        return readyForWritingFuture;
      } else {
        return (readyForWritingFuture = SettableFuture.create());
      }
    }
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    synchronized (lock) {
      if (!queue.isEmpty()) {
        return Futures.immediateFuture(null);
      } else if (readyForReadingFuture != null) {
        return readyForReadingFuture;
      } else {
        return (readyForReadingFuture = SettableFuture.create());
      }
    }
  }

  @Override
  public void doneWriting()
  {
    synchronized (lock) {
      if (isFinished()) {
        throw new ISE("Already done");
      }

      if (!queue.offer(END_MARKER)) {
        // If this happens, it's a bug, potentially due to incorrectly using this class with multiple writers.
        throw new ISE("Channel had capacity, but could not add end marker");
      }

      notifyReader();
    }
  }

  @Override
  public boolean isFinished()
  {
    synchronized (lock) {
      return END_MARKER.equals(queue.peek());
    }
  }

  @Override
  public boolean canRead()
  {
    synchronized (lock) {
      return !queue.isEmpty() && !isFinished();
    }
  }

  @Override
  public Try<Frame> read()
  {
    final Optional<Try<FrameWithPartition>> next;

    synchronized (lock) {
      if (isFinished()) {
        throw new NoSuchElementException();
      }

      next = queue.poll();

      if (next == null || !next.isPresent()) {
        throw new NoSuchElementException();
      }

      notifyWriter();
    }

    return next.get().map(FrameWithPartition::frame);
  }

  @Override
  public void doneReading()
  {
    synchronized (lock) {
      queue.clear();
      notifyWriter();
    }
  }

  @GuardedBy("lock")
  private void notifyReader()
  {
    if (readyForReadingFuture != null) {
      final SettableFuture<?> tmp = readyForReadingFuture;
      this.readyForReadingFuture = null;
      tmp.set(null);
    }
  }

  @GuardedBy("lock")
  private void notifyWriter()
  {
    if (readyForWritingFuture != null) {
      final SettableFuture<?> tmp = readyForWritingFuture;
      this.readyForWritingFuture = null;
      tmp.set(null);
    }
  }
}
