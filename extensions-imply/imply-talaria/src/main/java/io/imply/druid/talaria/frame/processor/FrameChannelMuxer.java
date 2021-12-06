/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.read.Frame;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FrameChannelMuxer implements FrameProcessor<Long>
{
  private final List<ReadableFrameChannel> inputChannels;
  private final WritableFrameChannel outputChannel;

  private final IntSet remainingChannels = new IntOpenHashSet();
  private long rowsRead = 0L;

  public FrameChannelMuxer(
      final List<ReadableFrameChannel> inputChannels,
      final WritableFrameChannel outputChannel
  )
  {
    this.inputChannels = inputChannels;
    this.outputChannel = outputChannel;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return inputChannels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (remainingChannels.isEmpty()) {
      // First run.
      for (int i = 0; i < inputChannels.size(); i++) {
        final ReadableFrameChannel channel = inputChannels.get(i);
        if (!channel.isFinished()) {
          remainingChannels.add(i);
        }
      }
    }

    if (!readableInputs.isEmpty()) {
      // Avoid biasing towards lower-numbered channels.
      final int channelIdx = ThreadLocalRandom.current().nextInt(readableInputs.size());

      int i = 0;
      for (IntIterator iterator = readableInputs.iterator(); iterator.hasNext(); i++) {
        final int channelNumber = iterator.nextInt();
        final ReadableFrameChannel channel = inputChannels.get(channelNumber);

        if (channel.isFinished()) {
          remainingChannels.remove(channelNumber);
        } else if (i == channelIdx) {
          final Frame frame = channel.read().getOrThrow();
          outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
          rowsRead += frame.numRows();
        }
      }
    }

    if (remainingChannels.isEmpty()) {
      return ReturnOrAwait.returnObject(rowsRead);
    } else {
      return ReturnOrAwait.awaitAny(remainingChannels);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }
}
