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
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.kernel.StageId;

import java.io.IOException;
import java.util.function.IntFunction;

public class CountingInputChannelFactory implements InputChannelFactory
{
  private final InputChannelFactory baseFactory;
  private final IntFunction<TalariaCounters.ChannelCounters> channelCountersFn;

  public CountingInputChannelFactory(
      InputChannelFactory baseFactory,
      IntFunction<TalariaCounters.ChannelCounters> channelCountersFn
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.channelCountersFn = Preconditions.checkNotNull(channelCountersFn, "channelCountersFn");
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException
  {
    return wrap(baseFactory.openChannel(stageId, workerNumber, partitionNumber), partitionNumber);
  }

  private ReadableFrameChannel wrap(final ReadableFrameChannel baseChannel, final int partitionNumber)
  {
    final TalariaCounters.ChannelCounters channelCounters = channelCountersFn.apply(partitionNumber);
    return new CountingReadableFrameChannel(baseChannel, channelCounters);
  }
}
