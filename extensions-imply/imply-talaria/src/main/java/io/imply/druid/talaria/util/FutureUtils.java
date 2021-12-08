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
import java.util.concurrent.ExecutionException;

/**
 * Future-related utility functions that would make sense in {@link org.apache.druid.common.guava.GuavaUtils} if this
 * were not an extension.
 */
public class FutureUtils
{
  /**
   * Waits for a given future and returns its value, like {@code future.get()}. The only difference is this method
   * will throw unchecked exceptions, instead of checked exceptions, if the future fails.
   */
  public static <T> T getUnchecked(final ListenableFuture<T> future)
  {
    try {
      return future.get();
    }
    catch (InterruptedException e) {
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
      return getUnchecked(future);
    } else if (future.isCancelled()) {
      throw new ISE("Cancelled");
    } else {
      throw new ISE("Not yet done");
    }
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
