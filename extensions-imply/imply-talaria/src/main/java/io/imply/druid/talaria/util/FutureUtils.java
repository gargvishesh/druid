/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Future-related utility functions that would make sense in {@link org.apache.druid.common.guava.GuavaUtils} if this
 * were not an extension.
 */
public class FutureUtils
{
  /**
   * Waits for a given future and returns its value, like {@code future.get()}. The only difference is this method
   * will throw unchecked exceptions, instead of checked exceptions, if the future fails.
   *
   * If "cancelIfInterrupted" is true, then this method will cancel the future if the current (waiting) thread
   * is interrupted.
   */
  public static <T> T getUnchecked(final ListenableFuture<T> future, final boolean cancelIfInterrupted)
  {
    try {
      return future.get();
    }
    catch (InterruptedException e) {
      if (cancelIfInterrupted) {
        future.cancel(true);
      }

      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the result of a given future immediately.
   *
   * Equivalent to {@link #getUnchecked} if the future is ready. Otherwise, throws an {@link IllegalStateException}.
   */
  public static <T> T getUncheckedImmediately(final ListenableFuture<T> future)
  {
    if (future.isDone()) {
      return getUnchecked(future, false);
    } else if (future.isCancelled()) {
      throw new ISE("Cancelled");
    } else {
      throw new ISE("Not yet done");
    }
  }

  /**
   * Similar to {@link Futures#allAsList}, but provides a "cancelOnErrorOrInterrupt" option that cancels all input
   * futures if the returned future is canceled or fails.
   */
  public static <T> ListenableFuture<List<T>> allAsList(
      final List<ListenableFuture<T>> futures,
      final boolean cancelOnErrorOrInterrupt
  )
  {
    final ListenableFuture<List<T>> retVal = Futures.allAsList(futures);

    if (cancelOnErrorOrInterrupt) {
      Futures.addCallback(
          retVal,
          new FutureCallback<List<T>>()
          {
            @Override
            public void onSuccess(@Nullable List<T> result)
            {
              // Do nothing.
            }

            @Override
            public void onFailure(Throwable t)
            {
              for (final ListenableFuture<T> inputFuture : futures) {
                inputFuture.cancel(true);
              }
            }
          }
      );
    }

    return retVal;
  }

  /**
   * Like {@link Futures#transform}, but works better with lambdas due to not having so many overloads.
   */
  public static <T, R> ListenableFuture<R> transform(final ListenableFuture<T> future, final Function<T, R> fn)
  {
    return Futures.transform(future, fn::apply);
  }

  /**
   * Returns a future that resolves when "future" resolves and "baggage" has been closed. If the baggage is closed
   * successfully, the returned future will have the same value (or exception status) as the input future. If the
   * baggage is not closed successfully, the returned future will resolve to an exception.
   */
  public static <T> ListenableFuture<T> futureWithBaggage(final ListenableFuture<T> future, final Closeable baggage)
  {
    final SettableFuture<T> retVal = SettableFuture.create();

    Futures.addCallback(
        future,
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(@Nullable T result)
          {
            try {
              baggage.close();
            }
            catch (Exception e) {
              retVal.setException(e);
              return;
            }

            retVal.set(result);
          }

          @Override
          public void onFailure(Throwable e)
          {
            try {
              baggage.close();
            }
            catch (Exception e2) {
              e.addSuppressed(e2);
            }

            retVal.setException(e);
          }
        }
    );

    return retVal;
  }
}
