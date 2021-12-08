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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * TODO(gianm): Better name: maybe LinkChannel or ChannelPair?
 */
public class OutputChannel
{
  @Nullable
  private final WritableFrameChannel writableChannel;
  private final Supplier<ReadableFrameChannel> readableChannelSupplier;
  private final int partitionNumber;

  public OutputChannel(
      @Nullable final WritableFrameChannel writableChannel,
      final Supplier<ReadableFrameChannel> readableChannelSupplier,
      final int partitionNumber
  )
  {
    this.writableChannel = writableChannel;
    this.readableChannelSupplier = readableChannelSupplier;
    this.partitionNumber = partitionNumber;

    if (partitionNumber < 0 && partitionNumber != FrameWithPartition.NO_PARTITION) {
      throw new IAE("Invalid partition number [%d]", partitionNumber);
    }
  }

  public WritableFrameChannel getWritableChannel()
  {
    if (writableChannel == null) {
      throw new ISE("Writable channel is not available");
    } else {
      return writableChannel;
    }
  }

  public ReadableFrameChannel getReadableChannel()
  {
    // TODO(gianm): block from being called if !streamable and writable channel still open
    return readableChannelSupplier.get();
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  public OutputChannel mapWritableChannel(final Function<WritableFrameChannel, WritableFrameChannel> mapFn)
  {
    if (writableChannel == null) {
      return this;
    } else {
      return new OutputChannel(mapFn.apply(writableChannel), readableChannelSupplier, partitionNumber);
    }
  }
}
