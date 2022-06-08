/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.MemoryRange;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.FrameComparisonWidget;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.read.FrameReaderUtils;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public class KeyStatisticsCollectionProcessor implements FrameProcessor<ClusterByStatisticsCollector>
{
  /**
   * Constant chosen such that a column full of "standard" values, with row count
   * {@link io.imply.druid.talaria.sql.TalariaQueryMaker#DEFAULT_ROWS_PER_SEGMENT}, and *some* redundancy between
   * rows (therefore: some "reasonable" compression) will not have any columns greater than 2GB in size.
   */
  private static final int STANDARD_VALUE_SIZE = 1000;

  /**
   * Constant chosen such that a segment full of "standard" rows, with row count
   * {@link io.imply.druid.talaria.sql.TalariaQueryMaker#DEFAULT_ROWS_PER_SEGMENT}, and *some* redundancy between
   * rows (therefore: some "reasonable" compression) will not be larger than 5GB in size.
   */
  private static final int STANDARD_ROW_SIZE = 2000;

  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final ClusterBy clusterBy;

  private ClusterByStatisticsCollector clusterByStatisticsCollector;

  public KeyStatisticsCollectionProcessor(
      final ReadableFrameChannel inputChannel,
      final WritableFrameChannel outputChannel,
      final FrameReader frameReader,
      final ClusterBy clusterBy,
      final ClusterByStatisticsCollector clusterByStatisticsCollector
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.clusterBy = clusterBy;
    this.clusterByStatisticsCollector = clusterByStatisticsCollector;
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
  public ReturnOrAwait<ClusterByStatisticsCollector> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(clusterByStatisticsCollector);
    }

    final Frame frame = inputChannel.read().getOrThrow();
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);
    final IntSupplier rowWeightSupplier = makeRowWeightSupplier(frameReader, cursor.getColumnSelectorFactory());
    final FrameComparisonWidget comparisonWidget = frameReader.makeComparisonWidget(frame, clusterBy.getColumns());

    for (int i = 0; i < frame.numRows(); i++, cursor.advance()) {
      final ClusterByKey key = comparisonWidget.readKey(i);
      clusterByStatisticsCollector.add(key, rowWeightSupplier.getAsInt());
    }

    // Clears partition info (uses NO_PARTITION), but that's OK, because it isn't needed downstream of this processor.
    outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
    return ReturnOrAwait.awaitAll(1);
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(
        inputChannels(),
        outputChannels(),
        () -> clusterByStatisticsCollector = null
    );
  }

  private IntSupplier makeRowWeightSupplier(
      final FrameReader frameReader,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    final Supplier<MemoryRange<Memory>> rowMemorySupplier =
        FrameReaderUtils.makeRowMemorySupplier(columnSelectorFactory, frameReader.signature());

    final int numFields = frameReader.signature().size();

    if (rowMemorySupplier == null) {
      // Can't access row memory.
      throw new ISE("Can't read row memory from frame. Wrong frame type or signature?");
    }

    return () -> {
      final MemoryRange<Memory> rowMemory = rowMemorySupplier.get();

      if (rowMemory == null) {
        // Can't access row memory.
        throw new ISE("Can't read row memory from frame. Wrong type or signature?");
      }

      long maxValueLength = 0;
      long totalLength = 0;
      long currentValueStartPosition = (long) Integer.BYTES * numFields;

      for (int i = 0; i < numFields; i++) {
        final long currentValueEndPosition = rowMemory.memory().getInt(rowMemory.start() + (long) Integer.BYTES * i);
        final long valueLength = currentValueEndPosition - currentValueStartPosition;

        if (valueLength > maxValueLength) {
          maxValueLength = valueLength;
        }

        totalLength += valueLength;
        currentValueStartPosition = currentValueEndPosition;
      }

      return 1 + Ints.checkedCast(
          Math.max(
              maxValueLength / STANDARD_VALUE_SIZE,
              totalLength / STANDARD_ROW_SIZE
          )
      );
    };
  }
}
