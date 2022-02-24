/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableNilFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents an output channel for some frame processor. Composed of a pair of {@link WritableFrameChannel}, which the
 * processor writes to, along with a supplier of a {@link ReadableFrameChannel}, which readers can read from.
 *
 * At the time an instance of this class is created, the writable channel is already open, but the readable channel
 * has not yet been created. It is created upon the first call to {@link #getReadableChannel()}.
 */
public class OutputChannel
{
  @Nullable
  private final WritableFrameChannel writableChannel;
  @Nullable
  private final MemoryAllocator frameMemoryAllocator;
  private final Supplier<ReadableFrameChannel> readableChannelSupplier;
  private final int partitionNumber;

  private OutputChannel(
      @Nullable final WritableFrameChannel writableChannel,
      @Nullable final MemoryAllocator frameMemoryAllocator,
      final Supplier<ReadableFrameChannel> readableChannelSupplier,
      final int partitionNumber
  )
  {
    this.writableChannel = writableChannel;
    this.frameMemoryAllocator = frameMemoryAllocator;
    this.readableChannelSupplier = readableChannelSupplier;
    this.partitionNumber = partitionNumber;

    if (partitionNumber < 0 && partitionNumber != FrameWithPartition.NO_PARTITION) {
      throw new IAE("Invalid partition number [%d]", partitionNumber);
    }
  }

  /**
   * Creates an output channel pair.
   *
   * @param writableChannel         writable channel for producer
   * @param frameMemoryAllocator    memory allocator for producer to use while writing frames to the channel
   * @param readableChannelSupplier readable channel for consumer. May be called multiple times, so you should wrap this
   *                                in {@link Suppliers#memoize} if needed.
   * @param partitionNumber         partition number, if any; may be {@link FrameWithPartition#NO_PARTITION} if unknown
   */
  public static OutputChannel pair(
      final WritableFrameChannel writableChannel,
      final MemoryAllocator frameMemoryAllocator,
      final Supplier<ReadableFrameChannel> readableChannelSupplier,
      final int partitionNumber
  )
  {
    return new OutputChannel(
        Preconditions.checkNotNull(writableChannel, "writableChannel"),
        Preconditions.checkNotNull(frameMemoryAllocator, "frameMemoryAllocator"),
        readableChannelSupplier,
        partitionNumber
    );
  }

  /**
   * Create a nil output channel, representing a processor that writes nothing. It is not actually writable, but
   * provides a way for downstream processors to read nothing.
   */
  public static OutputChannel nil(final int partitionNumber)
  {
    return new OutputChannel(null, null, () -> ReadableNilFrameChannel.INSTANCE, partitionNumber);
  }

  /**
   * Returns the writable channel of this pair. The producer writes to this channel.
   */
  public WritableFrameChannel getWritableChannel()
  {
    if (writableChannel == null) {
      throw new ISE("Writable channel is not available");
    } else {
      return writableChannel;
    }
  }

  /**
   * Returns the memory allocator for the writable channel. The producer uses this to generate frames for the channel.
   */
  public MemoryAllocator getFrameMemoryAllocator()
  {
    if (frameMemoryAllocator == null) {
      throw new ISE("Writable channel is not available");
    } else {
      return frameMemoryAllocator;
    }
  }

  /**
   * Returns the readable channel of this pair. This readable channel may, or may not, be usable before the
   * writable channel is closed. It depends on whether the channel pair was created in a stream-capable manner or not.
   */
  public ReadableFrameChannel getReadableChannel()
  {
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
      return new OutputChannel(
          mapFn.apply(writableChannel),
          frameMemoryAllocator,
          readableChannelSupplier,
          partitionNumber
      );
    }
  }
}
