/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.util;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ALL")
public class TestScheduledExecutorService implements ScheduledExecutorService
{
  @Override
  public ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      @Nonnull Runnable command,
      long initialDelay,
      long period,
      @Nonnull TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      @Nonnull Runnable command,
      long initialDelay,
      long delay,
      @Nonnull TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isShutdown()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTerminated()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Future<T> submit(@Nonnull Callable<T> task)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Future<T> submit(@Nonnull Runnable task, T result)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<?> submit(@Nonnull Runnable task)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      @Nonnull Collection<? extends Callable<T>> tasks,
      long timeout,
      @Nonnull TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(@Nonnull Runnable command)
  {
    throw new UnsupportedOperationException();
  }
}
