/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.common;

import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.HeapMemoryAllocator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class OrderByFrameProcessor implements FrameProcessor<Long>
{
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final VirtualColumns virtualColumns;
  private final RowSignature outputSignature;
  private final ClusterBy clusterBy;

  OrderByFrameProcessor(
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameReader frameReader,
      VirtualColumns virtualColumns,
      RowSignature outputSignature,
      ClusterBy clusterBy
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.virtualColumns = virtualColumns;
    this.frameReader = frameReader;
    this.outputSignature = outputSignature;
    this.clusterBy = clusterBy;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    } else if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(0L);
    }

    final Frame frame = inputChannel.read().getOrThrow();
    final Frame sortedFrame = sort(frame);
    outputChannel.write(new FrameWithPartition(sortedFrame, FrameWithPartition.NO_PARTITION));
    return ReturnOrAwait.awaitAll(1);
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }

  private Frame sort(final Frame frame)
  {
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);

    // TODO(gianm): hack to make sure we can always generate a single frame.
    //    we can remove this if we complexify the logic.
    final HeapMemoryAllocator unlimitedAllocator = HeapMemoryAllocator.unlimited();
    final ColumnSelectorFactory wrappedColumnSelectorFactory = virtualColumns.wrap(cursor.getColumnSelectorFactory());

    try (final FrameWriter frameWriter =
             FrameWriter.create(wrappedColumnSelectorFactory, unlimitedAllocator, outputSignature)) {
      while (!cursor.isDone()) {
        if (!frameWriter.addSelection()) {
          // Don't retry; it can't work because the allocator is unlimited anyway.
          // Also, I don't think this line can be reached, because the allocator is unlimited.
          throw new FrameRowTooLargeException(unlimitedAllocator.capacity());
        }

        cursor.advance();
      }

      frameWriter.sort(clusterBy.getColumns());
      return Frame.wrap(frameWriter.toByteArray());
    }
  }
}
