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
import io.imply.druid.talaria.counters.ChannelCounters;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.Try;

public class CountingReadableFrameChannel implements ReadableFrameChannel
{
  private final ReadableFrameChannel baseChannel;
  private final ChannelCounters channelCounters;
  private final int partitionNumber;

  public CountingReadableFrameChannel(
      ReadableFrameChannel baseChannel,
      ChannelCounters channelCounters,
      int partitionNumber
  )
  {
    this.baseChannel = baseChannel;
    this.channelCounters = channelCounters;
    this.partitionNumber = partitionNumber;
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
      channelCounters.addFrame(partitionNumber, frameTry.getOrThrow());
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
