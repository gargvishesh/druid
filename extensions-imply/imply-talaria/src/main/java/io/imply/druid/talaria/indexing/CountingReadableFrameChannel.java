/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.Try;
import io.imply.druid.talaria.frame.read.Frame;

public class CountingReadableFrameChannel implements ReadableFrameChannel
{
  private final ReadableFrameChannel baseChannel;
  private final TalariaCounters.ChannelCounters counters;

  public CountingReadableFrameChannel(
      ReadableFrameChannel baseChannel,
      TalariaCounters.ChannelCounters counters
  )
  {
    this.baseChannel = baseChannel;
    this.counters = counters;
  }

  @Override
  public boolean isFinished()
  {
    return baseChannel.isFinished();
  }

  @Override
  public boolean canRead()
  {
    return baseChannel.canRead();
  }

  @Override
  public Try<Frame> read()
  {
    final Try<Frame> frameTry = baseChannel.read();

    if (frameTry.isValue()) {
      counters.addFrame(frameTry.getOrThrow());
    }

    return frameTry;
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    return baseChannel.readabilityFuture();
  }

  @Override
  public void doneReading()
  {
    baseChannel.doneReading();
  }
}
