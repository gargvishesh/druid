/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import io.imply.druid.talaria.frame.channel.BlockingQueueFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableNilFrameChannel;

public class BlockingQueueOutputChannelFactory implements OutputChannelFactory
{
  public static final BlockingQueueOutputChannelFactory INSTANCE = new BlockingQueueOutputChannelFactory();

  private BlockingQueueOutputChannelFactory()
  {
    // Singleton.
  }

  @Override
  public OutputChannel openChannel(int partitionNumber, boolean sorted)
  {
    final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
    return new OutputChannel(channel, () -> channel, partitionNumber);
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return new OutputChannel(null, () -> ReadableNilFrameChannel.INSTANCE, partitionNumber);
  }
}
