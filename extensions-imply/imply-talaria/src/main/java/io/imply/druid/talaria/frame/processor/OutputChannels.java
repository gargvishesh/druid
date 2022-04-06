/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class OutputChannels
{
  private final List<OutputChannel> outputChannels;
  private final Int2ObjectSortedMap<List<OutputChannel>> partitionToChannelMap;

  private OutputChannels(final List<OutputChannel> outputChannels)
  {
    this.outputChannels = outputChannels;

    this.partitionToChannelMap = new Int2ObjectRBTreeMap<>();

    for (final OutputChannel outputChannel : outputChannels) {
      partitionToChannelMap.computeIfAbsent(outputChannel.getPartitionNumber(), ignored -> new ArrayList<>())
                           .add(outputChannel);
    }
  }

  public static OutputChannels none()
  {
    return wrap(Collections.emptyList());
  }

  /**
   * Creates an instance wrapping all the provided channels.
   */
  public static OutputChannels wrap(final List<OutputChannel> outputChannels)
  {
    return new OutputChannels(outputChannels);
  }

  /**
   * Creates an instance wrapping read-only versions (see {@link OutputChannel#readOnly()}) of all the
   * provided channels.
   */
  public static OutputChannels wrapReadOnly(final List<OutputChannel> outputChannels)
  {
    return new OutputChannels(outputChannels.stream().map(OutputChannel::readOnly).collect(Collectors.toList()));
  }

  public IntSortedSet getPartitionNumbers()
  {
    return partitionToChannelMap.keySet();
  }

  public List<OutputChannel> getAllChannels()
  {
    return outputChannels;
  }

  public List<OutputChannel> getChannelsForPartition(final int partitionNumber)
  {
    final List<OutputChannel> retVal = partitionToChannelMap.get(partitionNumber);

    if (retVal != null) {
      return retVal;
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns a read-only version of this instance. Each individual output channel is replaced with its
   * read-only version ({@link OutputChannel#readOnly()}, which reduces memory usage.
   */
  public OutputChannels readOnly()
  {
    return wrapReadOnly(outputChannels);
  }
}
