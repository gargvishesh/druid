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
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.Try;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;

import java.io.IOException;

public class CountingWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel baseChannel;
  private final TalariaCounters.ChannelCounters counters;

  public CountingWritableFrameChannel(
      final WritableFrameChannel baseChannel,
      final TalariaCounters.ChannelCounters counters
  )
  {
    this.baseChannel = baseChannel;
    this.counters = counters;
  }

  @Override
  public void write(Try<FrameWithPartition> frameTry) throws IOException
  {
    baseChannel.write(frameTry);

    if (frameTry.isValue()) {
      counters.addFrame(frameTry.getOrThrow().frame());
    }
  }

  @Override
  public void doneWriting() throws IOException
  {
    baseChannel.doneWriting();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return baseChannel.writabilityFuture();
  }
}
