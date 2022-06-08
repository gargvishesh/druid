/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.annotations.VisibleForTesting;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.indexing.error.BroadcastTablesTooLargeFault;
import io.imply.druid.talaria.indexing.error.TalariaException;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO(gianm): build hash tables using {@link org.apache.druid.query.groupby.epinephelinae.collection.MemoryOpenHashTable}
 */
public class BroadcastJoinHelper
{
  private final Int2IntMap sideStageChannelNumberMap;
  private final List<ReadableFrameChannel> channels;
  private final List<FrameReader> channelReaders;
  private final JoinableFactoryWrapper joinableFactory;
  private final List<List<Object[]>> channelData;
  private final IntSet sideChannelNumbers;
  private final long memoryReservedForBroadcastJoin;

  private long memoryUsed = 0L;

  /**
   * Create a new broadcast join helper.
   *
   * @param sideStageChannelNumberMap      map of input stage number -> channel position in the "channels" list
   * @param channels                       list of input channels
   * @param channelReaders                 list of input channel readers; corresponds one-to-one with "channels"
   * @param joinableFactory                joinable factory for this server
   * @param memoryReservedForBroadcastJoin total bytes of frames we are permitted to use; derived from
   *                                       {@link io.imply.druid.talaria.exec.WorkerMemoryParameters#broadcastJoinMemory}
   */
  public BroadcastJoinHelper(
      final Int2IntMap sideStageChannelNumberMap,
      final List<ReadableFrameChannel> channels,
      final List<FrameReader> channelReaders,
      final JoinableFactoryWrapper joinableFactory,
      final long memoryReservedForBroadcastJoin
  )
  {
    this.sideStageChannelNumberMap = sideStageChannelNumberMap;
    this.channels = channels;
    this.channelReaders = channelReaders;
    this.joinableFactory = joinableFactory;
    this.channelData = new ArrayList<>();
    this.sideChannelNumbers = new IntOpenHashSet();
    this.sideChannelNumbers.addAll(sideStageChannelNumberMap.values());
    this.memoryReservedForBroadcastJoin = memoryReservedForBroadcastJoin;

    for (int i = 0; i < channels.size(); i++) {
      if (sideChannelNumbers.contains(i)) {
        channelData.add(new ArrayList<>());
        sideChannelNumbers.add(i);
      } else {
        channelData.add(null);
      }
    }
  }

  /**
   * Reads up to one frame from each readable side channel, and uses them to incrementally build up joinable
   * broadcast tables.
   *
   * @param readableInputs all readable input channel numbers, including non-side-channels
   *
   * @return whether side channels have been fully read
   */
  public boolean buildBroadcastTablesIncrementally(final IntSet readableInputs)
  {
    final IntIterator inputChannelIterator = readableInputs.iterator();

    while (inputChannelIterator.hasNext()) {
      final int channelNumber = inputChannelIterator.nextInt();
      if (sideChannelNumbers.contains(channelNumber) && channels.get(channelNumber).canRead()) {
        final Frame frame = channels.get(channelNumber).read().getOrThrow();

        memoryUsed += frame.numBytes();

        if (memoryUsed > memoryReservedForBroadcastJoin) {
          throw new TalariaException(new BroadcastTablesTooLargeFault(memoryReservedForBroadcastJoin));
        }

        addFrame(channelNumber, frame);
      }
    }

    for (int channelNumber : sideChannelNumbers) {
      if (!channels.get(channelNumber).isFinished()) {
        return false;
      }
    }

    return true;
  }

  public IntSet getSideChannelNumbers()
  {
    return sideChannelNumbers;
  }

  public Function<SegmentReference, SegmentReference> makeSegmentMapFn(final Query<?> query)
  {
    final DataSource dataSourceWithInlinedChannelData = inlineChannelData(query.getDataSource());
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(dataSourceWithInlinedChannelData);

    return joinableFactory.createSegmentMapFn(
        analysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null),
        analysis.getPreJoinableClauses(),
        new AtomicLong() /* TODO(gianm): cpu time accumulator */,
        analysis.getBaseQuery().orElse(query)
    );
  }

  @VisibleForTesting
  DataSource inlineChannelData(final DataSource originalDataSource)
  {
    if (originalDataSource instanceof InputStageDataSource) {
      final int stageNumber = ((InputStageDataSource) originalDataSource).getStageNumber();
      if (sideStageChannelNumberMap.containsKey(stageNumber)) {
        final int channelNumber = sideStageChannelNumberMap.get(stageNumber);

        if (sideChannelNumbers.contains(channelNumber)) {
          return InlineDataSource.fromIterable(
              channelData.get(channelNumber),
              channelReaders.get(channelNumber).signature()
          );
        } else {
          return originalDataSource;
        }
      } else {
        return originalDataSource;
      }
    } else {
      final List<DataSource> newChildren = new ArrayList<>(originalDataSource.getChildren().size());

      for (final DataSource child : originalDataSource.getChildren()) {
        newChildren.add(inlineChannelData(child));
      }

      return originalDataSource.withChildren(newChildren);
    }
  }

  private void addFrame(final int channelNumber, final Frame frame)
  {
    final List<Object[]> data = channelData.get(channelNumber);
    final FrameReader frameReader = channelReaders.get(channelNumber);
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);

    final List<ColumnValueSelector> selectors =
        frameReader.signature().getColumnNames().stream().map(
            columnName ->
                cursor.getColumnSelectorFactory().makeColumnValueSelector(columnName)
        ).collect(Collectors.toList());

    while (!cursor.isDone()) {
      final Object[] row = new Object[selectors.size()];
      for (int i = 0; i < row.length; i++) {
        row[i] = selectors.get(i).getObject();
      }
      data.add(row);
      cursor.advance();
    }
  }
}
