/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.groupby;

import com.google.common.collect.Iterables;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameSegment;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.querykit.BaseLeafFrameProcessor;
import io.imply.druid.talaria.querykit.QueryWorkerInput;
import io.imply.druid.talaria.querykit.SegmentWithInterval;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;

/**
 * A {@link FrameProcessor} that reads one {@link Frame} at a time from a particular segment, writes them
 * to a {@link WritableFrameChannel}, and returns the number of rows output.
 */
public class GroupByPreShuffleFrameProcessor extends BaseLeafFrameProcessor
{
  private final GroupByQuery query;
  private final GroupByStrategySelector strategySelector;
  private final RowSignature aggregationSignature;
  private final ClusterBy clusterBy;
  private final ColumnSelectorFactory frameWriterColumnSelectorFactory;
  private final Closer closer = Closer.create();

  private Yielder<ResultRow> resultYielder;
  private FrameWriter frameWriter;
  private long rowsOutput;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed

  public GroupByPreShuffleFrameProcessor(
      final GroupByQuery query,
      final QueryWorkerInput baseInput,
      final Int2ObjectMap<ReadableFrameChannel> sideChannels,
      final Int2ObjectMap<FrameReader> sideChannelReaders,
      final GroupByStrategySelector strategySelector,
      final JoinableFactoryWrapper joinableFactory,
      final RowSignature aggregationSignature,
      final ClusterBy clusterBy,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<MemoryAllocator> allocator,
      final long memoryReservedForBroadcastJoin
  )
  {
    super(
        query,
        baseInput,
        sideChannels,
        sideChannelReaders,
        joinableFactory,
        outputChannel,
        allocator,
        memoryReservedForBroadcastJoin
    );
    this.query = query;
    this.strategySelector = strategySelector;
    this.aggregationSignature = aggregationSignature;
    this.clusterBy = clusterBy;
    this.frameWriterColumnSelectorFactory = RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
        query,
        () -> resultYielder.get(),
        RowSignature.Finalization.NO
    );
  }

  @Override
  protected ReturnOrAwait<Long> runWithSegment(final SegmentWithInterval segment) throws IOException
  {
    if (resultYielder == null) {
      closer.register(segment);

      final Sequence<ResultRow> rowSequence =
          strategySelector.strategize(query)
                          .process(
                              query.withQuerySegmentSpec(new SpecificSegmentSpec(segment.toDescriptor())),
                              mapSegment(segment.getOrLoadSegment()).asStorageAdapter()
                          );

      resultYielder = Yielders.each(rowSequence);
    }

    populateFrameWriterAndFlushIfNeeded();

    if (resultYielder == null || resultYielder.isDone()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  protected ReturnOrAwait<Long> runWithInputChannel(
      final ReadableFrameChannel inputChannel,
      final FrameReader inputFrameReader
  ) throws IOException
  {
    if (resultYielder == null || resultYielder.isDone()) {
      closeAndDiscardResultYielder();

      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read().getOrThrow();
        final FrameSegment frameSegment = new FrameSegment(frame, inputFrameReader, SegmentId.dummy("x"));

        final Sequence<ResultRow> rowSequence =
            strategySelector.strategize(query)
                            .process(
                                query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY)),
                                mapSegment(frameSegment).asStorageAdapter()
                            );

        resultYielder = Yielders.each(rowSequence);
      } else if (inputChannel.isFinished()) {
        flushFrameWriterIfNeeded();
        return ReturnOrAwait.returnObject(rowsOutput);
      } else {
        return ReturnOrAwait.awaitAll(inputChannels().size());
      }
    }

    // Cursor has some more data in it.
    populateFrameWriterAndFlushIfNeeded();

    if (resultYielder == null || resultYielder.isDone()) {
      closeAndDiscardResultYielder();
      return ReturnOrAwait.awaitAll(inputChannels().size());
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    closer.register(this::closeAndDiscardResultYielder);
    closer.register(frameWriter);
    closer.register(super::cleanup);
    closer.close();
  }

  private void populateFrameWriterAndFlushIfNeeded() throws IOException
  {
    createFrameWriterIfNeeded();

    while (!resultYielder.isDone()) {
      final boolean didAddToFrame = frameWriter.addSelection();

      if (didAddToFrame) {
        resultYielder = resultYielder.next(null);
      } else if (frameWriter.getNumRows() == 0) {
        throw new FrameRowTooLargeException(currentAllocatorCapacity);
      } else {
        flushFrameWriterIfNeeded();
        return;
      }
    }

    flushFrameWriterIfNeeded();
    closeAndDiscardResultYielder();
  }

  private void createFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      final MemoryAllocator allocator = getAllocator();
      frameWriter = FrameWriter.create(frameWriterColumnSelectorFactory, allocator, aggregationSignature);
      currentAllocatorCapacity = allocator.capacity();
    }
  }

  private void flushFrameWriterIfNeeded() throws IOException
  {
    if (frameWriter != null && frameWriter.getNumRows() > 0) {
      frameWriter.sort(clusterBy.getColumns());
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      Iterables.getOnlyElement(outputChannels()).write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
      rowsOutput += frame.numRows();
    }
  }

  private void closeAndDiscardResultYielder() throws IOException
  {
    final Yielder<ResultRow> tmp = resultYielder;
    resultYielder = null;

    if (tmp != null) {
      tmp.close();
    }
  }
}
