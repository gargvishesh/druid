/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Bouncer
{
  private final int maxCount;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private int currentCount = 0;

  @GuardedBy("lock")
  private final Queue<SettableFuture<Ticket>> waiters = new ArrayDeque<>();

  public Bouncer(final int maxCount)
  {
    this.maxCount = maxCount;

    if (maxCount <= 0) {
      throw new ISE("maxConcurrentWorkers must be greater than zero");
    }
  }

  public static Bouncer unlimited()
  {
    return new Bouncer(Integer.MAX_VALUE);
  }

  public int getMaxCount()
  {
    return maxCount;
  }

  public ListenableFuture<Ticket> ticket()
  {
    synchronized (lock) {
      if (currentCount < maxCount) {
        currentCount++;
        //noinspection UnstableApiUsage
        return Futures.immediateFuture(new Ticket());
      } else {
        final SettableFuture<Ticket> future = SettableFuture.create();
        waiters.add(future);
        return future;
      }
    }
  }

  public class Ticket
  {
    private final AtomicBoolean givenBack = new AtomicBoolean();

    public void giveBack()
    {
      if (!givenBack.compareAndSet(false, true)) {
        return;
      }

      // Loop to find a new home for this ticket that isn't canceled.

      while (true) {
        final SettableFuture<Ticket> nextFuture;

        synchronized (lock) {
          nextFuture = waiters.poll();

          if (nextFuture == null) {
            // Nobody was waiting.
            currentCount--;
            return;
          }
        }

        if (nextFuture.set(new Ticket())) {
          return;
        }
      }
    }
  }
}
