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
import io.imply.druid.talaria.counters.ChannelCounters;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;

import java.io.IOException;

public class CountingOutputChannelFactory implements OutputChannelFactory
{
  private final OutputChannelFactory baseFactory;
  private final ChannelCounters channelCounters;

  public CountingOutputChannelFactory(
      final OutputChannelFactory baseFactory,
      final ChannelCounters channelCounters
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.channelCounters = Preconditions.checkNotNull(channelCounters, "channelCounter");
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    final OutputChannel baseChannel = baseFactory.openChannel(partitionNumber);

    return baseChannel.mapWritableChannel(
        baseWritableChannel ->
            new CountingWritableFrameChannel(
                baseChannel.getWritableChannel(),
                channelCounters,
                baseChannel.getPartitionNumber()
            )
    );
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    // No need for counters on nil channels: they never receive input.
    return baseFactory.openNilChannel(partitionNumber);
  }
}
