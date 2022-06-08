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
import io.imply.druid.talaria.frame.Frame;

import java.util.NoSuchElementException;

/**
 * TODO(gianm): Javadocs
 */
public class ReadableNilFrameChannel implements ReadableFrameChannel
{
  public static final ReadableNilFrameChannel INSTANCE = new ReadableNilFrameChannel();

  private ReadableNilFrameChannel()
  {
  }

  @Override
  public boolean isFinished()
  {
    return true;
  }

  @Override
  public boolean canRead()
  {
    return false;
  }

  @Override
  public Try<Frame> read()
  {
    throw new NoSuchElementException();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    return Futures.immediateFuture(null);
  }

  @Override
  public void doneReading()
  {
    // Nothing to do.
  }
}
