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
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.HeapMemoryAllocator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.Cursor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LimitFrameProcessor implements FrameProcessor<Long>
{
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final long limit;

  long rowsOutput = 0L;

  LimitFrameProcessor(
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameReader frameReader,
      long limit
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.limit = limit;
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
      return ReturnOrAwait.returnObject(rowsOutput);
    }

    final Frame frame = inputChannel.read().getOrThrow();
    final Frame truncatedFrame = truncate(frame, frameReader, Math.min(frame.numRows(), limit - rowsOutput));
    outputChannel.write(new FrameWithPartition(truncatedFrame, FrameWithPartition.NO_PARTITION));
    rowsOutput += truncatedFrame.numRows();

    if (rowsOutput == limit) {
      return ReturnOrAwait.returnObject(rowsOutput);
    } else {
      return ReturnOrAwait.awaitAll(1);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }

  private static Frame truncate(final Frame frame, final FrameReader frameReader, final long numRows)
  {
    if (numRows <= 0 || numRows > frame.numRows()) {
      throw new ISE("Invalid numRows [%d]", numRows);
    } else if (numRows == frame.numRows()) {
      return frame;
    }

    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);

    // TODO(gianm): hack to make sure we can always generate a single frame.
    //    we can remove this need if we complexify the logic.
    final HeapMemoryAllocator unlimitedAllocator = HeapMemoryAllocator.unlimited();

    long rowsSoFar = 0;
    try (final FrameWriter frameWriter =
             FrameWriter.create(cursor.getColumnSelectorFactory(), unlimitedAllocator, frameReader.signature())) {
      while (!cursor.isDone() && rowsSoFar < numRows) {
        if (!frameWriter.addSelection()) {
          // Don't retry; it can't work because the allocator is unlimited anyway. The individual row must have
          // been too large on its own.
          throw new FrameRowTooLargeException();
        }

        cursor.advance();
        rowsSoFar++;
      }

      return Frame.wrap(frameWriter.toByteArray());
    }
  }
}
