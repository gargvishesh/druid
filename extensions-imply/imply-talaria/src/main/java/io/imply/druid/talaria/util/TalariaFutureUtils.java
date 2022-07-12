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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;

/**
 * Future-related utility functions that haven't been moved to {@link FutureUtils}.
 */
public class TalariaFutureUtils
{
  /**
   * Gets the result of a given future immediately.
   *
   * Equivalent to {@link FutureUtils#getUnchecked} if the future is ready.
   * Otherwise, throws an {@link IllegalStateException}.
   */
  public static <T> T getUncheckedImmediately(final ListenableFuture<T> future)
  {
    if (future.isDone()) {
      return FutureUtils.getUnchecked(future, false);
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
