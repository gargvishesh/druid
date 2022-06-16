/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.ISE;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Response object for {@link FrameFileHttpResponseHandler}.
 */
public class FrameFilePartialFetch
{
  private final AtomicLong bytesRead = new AtomicLong(0L);
  private final AtomicReference<Throwable> exceptionCaught = new AtomicReference<>();
  private final AtomicReference<ListenableFuture<?>> backpressureFuture = new AtomicReference<>();

  FrameFilePartialFetch()
  {
  }

  public boolean isEmptyFetch()
  {
    return exceptionCaught.get() == null && bytesRead.get() == 0L;
  }

  /**
   * The exception that was encountered, if {@link #isExceptionCaught()} is true.
   *
   * @throws IllegalStateException if no exception was caught
   */
  public Throwable getExceptionCaught()
  {
    if (!isExceptionCaught()) {
      throw new ISE("No exception caught");
    }

    return exceptionCaught.get();
  }

  /**
   * Whether an exception was encountered during response processing.
   */
  public boolean isExceptionCaught()
  {
    return exceptionCaught.get() != null;
  }

  /**
   * Future that resolves when it is a good time to request the next chunk of the frame file.
   */
  public ListenableFuture<?> backpressureFuture()
  {
    final ListenableFuture<?> future = backpressureFuture.getAndSet(null);
    if (future != null) {
      return future;
    } else {
      return Futures.immediateFuture(null);
    }
  }

  void setBackpressureFuture(final ListenableFuture<?> future)
  {
    backpressureFuture.compareAndSet(null, future);
  }

  void exceptionCaught(final Throwable t)
  {
    exceptionCaught.compareAndSet(null, t);
  }

  void addBytesRead(final long n)
  {
    bytesRead.addAndGet(n);
  }
}
