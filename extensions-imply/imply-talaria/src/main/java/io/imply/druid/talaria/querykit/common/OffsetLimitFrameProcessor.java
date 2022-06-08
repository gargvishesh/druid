/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.common;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.HeapMemoryAllocator;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.FrameWriterFactory;
import io.imply.druid.talaria.frame.write.FrameWriters;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.Cursor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class OffsetLimitFrameProcessor implements FrameProcessor<Long>
{
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final long offset;
  private final long limit;

  long rowsProcessedSoFar = 0L;

  OffsetLimitFrameProcessor(
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameReader frameReader,
      long offset,
      long limit
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.offset = offset;
    this.limit = limit;

    if (offset < 0 || limit < 0) {
      throw new ISE("Offset and limit must be nonnegative");
    }
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
    } else if (inputChannel.isFinished() || rowsProcessedSoFar == offset + limit) {
      return ReturnOrAwait.returnObject(rowsProcessedSoFar);
    }

    final Frame frame = inputChannel.read().getOrThrow();
    final Frame truncatedFrame = chopAndProcess(frame, frameReader);

    if (truncatedFrame != null) {
      outputChannel.write(new FrameWithPartition(truncatedFrame, FrameWithPartition.NO_PARTITION));
    }

    if (rowsProcessedSoFar == offset + limit) {
      // This check is not strictly necessary, given the check above, but prevents one extra scheduling round.
      return ReturnOrAwait.returnObject(rowsProcessedSoFar);
    } else {
      assert rowsProcessedSoFar < offset + limit;
      return ReturnOrAwait.awaitAll(1);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }

  /**
   * Chops a frame down to a smaller one, potentially on both ends.
   *
   * Increments {@link #rowsProcessedSoFar} as it does its work. Either returns the original frame, a chopped frame,
   * or null if no rows from the current frame should be included.
   */
  @Nullable
  private Frame chopAndProcess(final Frame frame, final FrameReader frameReader)
  {
    final long startRow = Math.max(0, offset - rowsProcessedSoFar);
    final long endRow = Math.min(frame.numRows(), offset + limit - rowsProcessedSoFar);

    if (startRow >= endRow) {
      // Offset is past the end of the frame; skip it.
      rowsProcessedSoFar += frame.numRows();
      return null;
    } else if (startRow == 0 && endRow == frame.numRows()) {
      rowsProcessedSoFar += frame.numRows();
      return frame;
    }

    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);

    // TODO(gianm): hack to make sure we can always generate a single frame.
    //    we can remove this need if we complexify the logic.
    final HeapMemoryAllocator unlimitedAllocator = HeapMemoryAllocator.unlimited();

    long rowsProcessedSoFarInFrame = 0;

    final FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        unlimitedAllocator,
        frameReader.signature(),
        Collections.emptyList()
    );

    try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(cursor.getColumnSelectorFactory())) {
      while (!cursor.isDone() && rowsProcessedSoFarInFrame < endRow) {
        if (rowsProcessedSoFarInFrame >= startRow && !frameWriter.addSelection()) {
          // Don't retry; it can't work because the allocator is unlimited anyway.
          // Also, I don't think this line can be reached, because the allocator is unlimited.
          throw new FrameRowTooLargeException(unlimitedAllocator.capacity());
        }

        cursor.advance();
        rowsProcessedSoFarInFrame++;
      }

      rowsProcessedSoFar += rowsProcessedSoFarInFrame;
      return Frame.wrap(frameWriter.toByteArray());
    }
  }
}
