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
import io.imply.druid.talaria.frame.write.ArenaMemoryAllocator;

public class BlockingQueueOutputChannelFactory implements OutputChannelFactory
{
  private final int frameSize;

  public BlockingQueueOutputChannelFactory(final int frameSize)
  {
    this.frameSize = frameSize;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber)
  {
    final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
    return OutputChannel.pair(channel, ArenaMemoryAllocator.createOnHeap(frameSize), () -> channel, partitionNumber);
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }
}
