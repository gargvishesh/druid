/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.read.FrameComparisonWidget;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.row.FrameColumnSelectorFactory;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.FrameWriterFactory;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class FrameChannelMerger implements FrameProcessor<Long>
{
  static final long UNLIMITED = -1;

  private final List<ReadableFrameChannel> inputChannels;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final ClusterBy clusterBy;
  private final ClusterByPartitions partitions;
  private final IntPriorityQueue priorityQueue;
  private final FrameWriterFactory frameWriterFactory;
  private final FramePlus[] currentFrames;
  private final long rowLimit;
  private long rowsOutput = 0;
  private int currentPartition = 0;

  // ColumnSelectorFactory that always reads from the current row in the merged sequence.
  final MultiColumnSelectorFactory mergedColumnSelectorFactory;

  public FrameChannelMerger(
      final List<ReadableFrameChannel> inputChannels,
      final FrameReader frameReader,
      final WritableFrameChannel outputChannel,
      final FrameWriterFactory frameWriterFactory,
      final ClusterBy clusterBy,
      @Nullable final ClusterByPartitions partitions,
      final long rowLimit
  )
  {
    if (inputChannels.isEmpty()) {
      throw new IAE("Must have at least one input channel");
    }

    final ClusterByPartitions partitionsToUse =
        partitions == null ? ClusterByPartitions.oneUniversalPartition() : partitions;

    if (!partitionsToUse.allAbutting()) {
      // Sanity check: we lack logic in FrameMergeIterator for not returning every row in the provided frames, so make
      // sure there are no holes in partitionsToUse. Note that this check isn't perfect, because rows outside the
      // min / max value of partitionsToUse can still appear. But it's a cheap check, and it doesn't hurt to do it.
      throw new IAE("Partitions must all abut each other");
    }

    this.inputChannels = inputChannels;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.frameWriterFactory = frameWriterFactory;
    this.clusterBy = clusterBy;
    this.partitions = partitionsToUse;
    this.rowLimit = rowLimit;
    this.currentFrames = new FramePlus[inputChannels.size()];
    this.priorityQueue = new IntHeapPriorityQueue(
        inputChannels.size(),
        (k1, k2) -> currentFrames[k1].comparisonWidget.compare(
            currentFrames[k1].rowNumber,
            currentFrames[k2].comparisonWidget,
            currentFrames[k2].rowNumber
        )
    );

    final List<Supplier<ColumnSelectorFactory>> frameColumnSelectorFactorySuppliers =
        new ArrayList<>(inputChannels.size());

    for (int i = 0; i < inputChannels.size(); i++) {
      final int frameNumber = i;
      frameColumnSelectorFactorySuppliers.add(() -> currentFrames[frameNumber].cursor.getColumnSelectorFactory());
    }

    this.mergedColumnSelectorFactory =
        new MultiColumnSelectorFactory(
            frameColumnSelectorFactorySuppliers,

            // Include ROW_SIGNATURE_COLUMN, ROW_MEMORY_COLUMN to potentially enable direct row memory copying.
            // If these columns don't actually exist in the underlying column selector factories, they'll be ignored.
            RowSignature.builder()
                        .addAll(frameReader.signature())
                        .add(FrameColumnSelectorFactory.ROW_SIGNATURE_COLUMN, ColumnType.UNKNOWN_COMPLEX)
                        .add(FrameColumnSelectorFactory.ROW_MEMORY_COLUMN, ColumnType.UNKNOWN_COMPLEX)
                        .build()
        );
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
    final IntSet awaitSet = populateCurrentFramesAndPriorityQueue();

    if (!awaitSet.isEmpty()) {
      return ReturnOrAwait.awaitAll(awaitSet);
    }

    if (priorityQueue.isEmpty()) {
      // Done!
      return ReturnOrAwait.returnObject(rowsOutput);
    }

    // Generate one output frame and stop for now.
    outputChannel.write(nextFrame());
    return ReturnOrAwait.runAgain();
  }

  private FrameWithPartition nextFrame()
  {
    if (priorityQueue.isEmpty()) {
      throw new NoSuchElementException();
    }

    try (final FrameWriter mergedFrameWriter = frameWriterFactory.newFrameWriter(mergedColumnSelectorFactory)) {
      int mergedFramePartition = currentPartition;
      ClusterByKey currentPartitionEnd = partitions.get(currentPartition).getEnd();

      while (!priorityQueue.isEmpty()) {
        final int currentChannel = priorityQueue.firstInt();
        mergedColumnSelectorFactory.setCurrentFactory(currentChannel);

        if (currentPartitionEnd != null) {
          final FramePlus currentFrame = currentFrames[currentChannel];
          if (currentFrame.comparisonWidget.compare(currentFrame.rowNumber, currentPartitionEnd) >= 0) {
            // Current key is past the end of the partition. Advance currentPartition til it matches the current key.
            do {
              currentPartition++;
              currentPartitionEnd = partitions.get(currentPartition).getEnd();
            } while (currentPartitionEnd != null
                     && currentFrame.comparisonWidget.compare(currentFrame.rowNumber, currentPartitionEnd) >= 0);

            if (mergedFrameWriter.getNumRows() == 0) {
              // Fall through: keep reading into the new partition.
              mergedFramePartition = currentPartition;
            } else {
              // Return current frame.
              break;
            }
          }
        }

        if (mergedFrameWriter.addSelection()) {
          rowsOutput++;
        } else {
          if (mergedFrameWriter.getNumRows() == 0) {
            throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
          }

          // Frame is full. Don't touch the priority queue; instead, return the current frame.
          break;
        }

        if (rowLimit != UNLIMITED && rowsOutput >= rowLimit) {
          // Limit reached; we're done.
          priorityQueue.clear();
          Arrays.fill(currentFrames, null);
        } else {
          // Continue populating the priority queue.
          if (currentChannel != priorityQueue.dequeueInt()) {
            // There's a bug in this function. Nothing sensible we can really include in this error message.
            throw new ISE("Unexpected channel");
          }

          final FramePlus channelFramePlus = currentFrames[currentChannel];
          channelFramePlus.advance();

          if (!channelFramePlus.cursor.isDone()) {
            // Add this channel back to the priority queue, so it pops back out at the right time.
            priorityQueue.enqueue(currentChannel);
          } else {
            // Done reading current frame from "channel".
            // Clear it and see if there is another one available for immediate loading.
            currentFrames[currentChannel] = null;

            final ReadableFrameChannel channel = inputChannels.get(currentChannel);

            if (channel.canRead()) {
              // Read next frame from this channel.
              final Frame frame = channel.read().getOrThrow();
              currentFrames[currentChannel] = new FramePlus(frame, frameReader, clusterBy);
              priorityQueue.enqueue(currentChannel);
            } else if (channel.isFinished()) {
              // Done reading this channel. Fall through and continue with other channels.
            } else {
              // Nothing available, not finished; we can't continue. Finish up the current frame and return it.
              break;
            }
          }
        }
      }

      final Frame nextFrame = Frame.wrap(mergedFrameWriter.toByteArray());
      return new FrameWithPartition(nextFrame, mergedFramePartition);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }

  /**
   * Populates {@link #currentFrames}, wherever necessary, from any readable input channels. Returns the set of
   * channels that are required for population but are not readable.
   */
  private IntSet populateCurrentFramesAndPriorityQueue()
  {
    final IntSet await = new IntOpenHashSet();

    for (int i = 0; i < inputChannels.size(); i++) {
      if (currentFrames[i] == null) {
        final ReadableFrameChannel channel = inputChannels.get(i);

        if (channel.canRead()) {
          final Frame frame = channel.read().getOrThrow();
          currentFrames[i] = new FramePlus(frame, frameReader, clusterBy);
          priorityQueue.enqueue(i);
        } else if (!channel.isFinished()) {
          await.add(i);
        }
      }
    }

    return await;
  }

  /**
   * Class that encapsulates the apparatus necessary for reading a {@link Frame}.
   */
  private static class FramePlus
  {
    private final Cursor cursor;
    private final FrameComparisonWidget comparisonWidget;
    private int rowNumber;

    private FramePlus(Frame frame, FrameReader frameReader, ClusterBy clusterBy)
    {
      this.cursor = FrameProcessors.makeCursor(frame, frameReader);
      this.comparisonWidget = frameReader.makeComparisonWidget(frame, clusterBy.getColumns());
      this.rowNumber = 0;
    }

    private void advance()
    {
      cursor.advance();
      rowNumber++;
    }
  }
}
