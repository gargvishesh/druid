/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;

import java.io.IOException;
import java.util.function.IntFunction;

public class CountingOutputChannelFactory implements OutputChannelFactory
{
  private final OutputChannelFactory baseFactory;
  private final IntFunction<TalariaCounters.ChannelCounters> channelCountersFn;

  public CountingOutputChannelFactory(
      final OutputChannelFactory baseFactory,
      final IntFunction<TalariaCounters.ChannelCounters> channelCountersFn
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.channelCountersFn = Preconditions.checkNotNull(channelCountersFn, "channelCountersFn");
  }

  @Override
  public OutputChannel openChannel(int partitionNumber, boolean sorted) throws IOException
  {
    return wrap(baseFactory.openChannel(partitionNumber, sorted));
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return wrap(baseFactory.openNilChannel(partitionNumber));
  }

  private OutputChannel wrap(final OutputChannel baseChannel)
  {
    final TalariaCounters.ChannelCounters channelCounters = channelCountersFn.apply(baseChannel.getPartitionNumber());
    return baseChannel.mapWritableChannel(
        baseWritableChannel ->
            new CountingWritableFrameChannel(baseWritableChannel, channelCounters)
    );
  }
}
