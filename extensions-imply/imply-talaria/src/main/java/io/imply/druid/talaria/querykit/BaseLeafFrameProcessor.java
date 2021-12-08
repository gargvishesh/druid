/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.FrameReader;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class BaseLeafFrameProcessor implements FrameProcessor<Long>
{
  private final Query<?> query;
  private final QueryWorkerInput baseInput;
  private final List<ReadableFrameChannel> inputChannels;
  private final ResourceHolder<WritableFrameChannel> outputChannel;
  private final ResourceHolder<MemoryAllocator> allocator;
  private final BroadcastJoinHelper broadcastJoinHelper;

  private Function<SegmentReference, SegmentReference> segmentMapFn;

  protected BaseLeafFrameProcessor(
      final Query<?> query,
      final QueryWorkerInput baseInput,
      final Int2ObjectMap<ReadableFrameChannel> sideChannels,
      final Int2ObjectMap<FrameReader> sideChannelReaders,
      final JoinableFactoryWrapper joinableFactory,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<MemoryAllocator> allocator
  )
  {
    this.query = query;
    this.baseInput = baseInput;
    this.outputChannel = outputChannel;
    this.allocator = allocator;

    final Pair<List<ReadableFrameChannel>, BroadcastJoinHelper> inputChannelsAndBroadcastJoinHelper =
        makeInputChannelsAndBroadcastJoinHelper(
            query.getDataSource(),
            baseInput,
            sideChannels,
            sideChannelReaders,
            joinableFactory
        );

    this.inputChannels = inputChannelsAndBroadcastJoinHelper.lhs;
    this.broadcastJoinHelper = inputChannelsAndBroadcastJoinHelper.rhs;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return inputChannels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel.get());
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (!initializeSegmentMapFn(readableInputs)) {
      return ReturnOrAwait.awaitAll(broadcastJoinHelper.getSideChannelNumbers());
    } else if (readableInputs.size() != inputChannels.size()) {
      return ReturnOrAwait.awaitAll(inputChannels.size());
    } else if (baseInput.hasSegment()) {
      return runWithSegment(baseInput.getSegment());
    } else {
      return runWithInputChannel(baseInput.getInputChannel(), baseInput.getInputFrameReader());
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    // Don't close the output channel, because multiple workers write to the same channel.
    // The channel should be closed by the caller.
    FrameProcessors.closeAll(inputChannels(), Collections.emptyList(), outputChannel, allocator);
  }

  protected MemoryAllocator getAllocator()
  {
    return allocator.get();
  }

  protected abstract ReturnOrAwait<Long> runWithSegment(SegmentWithInterval segment) throws IOException;

  protected abstract ReturnOrAwait<Long> runWithInputChannel(
      ReadableFrameChannel inputChannel,
      FrameReader inputFrameReader
  ) throws IOException;

  /**
   * Helper intended to be used by subclasses. Applies {@link #segmentMapFn}, which applies broadcast joins
   * if applicable to this query.
   */
  protected SegmentReference mapSegment(final Segment segment)
  {
    return segmentMapFn.apply(ReferenceCountingSegment.wrapRootGenerationSegment(segment));
  }

  private boolean initializeSegmentMapFn(final IntSet readableInputs)
  {
    if (segmentMapFn != null) {
      return true;
    } else if (broadcastJoinHelper == null) {
      segmentMapFn = Function.identity();
      return true;
    } else {
      final boolean retVal = broadcastJoinHelper.buildBroadcastTablesIncrementally(readableInputs);

      if (retVal) {
        segmentMapFn = broadcastJoinHelper.makeSegmentMapFn(query);
      }

      return retVal;
    }
  }

  /**
   * Helper that enables implementations of {@link BaseLeafFrameProcessorFactory} to set up their primary and side channels.
   */
  private static Pair<List<ReadableFrameChannel>, BroadcastJoinHelper> makeInputChannelsAndBroadcastJoinHelper(
      final DataSource dataSource,
      final QueryWorkerInput baseInput,
      final Int2ObjectMap<ReadableFrameChannel> sideChannels,
      final Int2ObjectMap<FrameReader> sideChannelReaders,
      final JoinableFactoryWrapper joinableFactory
  )
  {
    if (!(dataSource instanceof JoinDataSource) && !sideChannels.isEmpty()) {
      throw new ISE("Did not expect side channels for dataSource [%s]", dataSource);
    }

    final List<ReadableFrameChannel> inputChannels = new ArrayList<>();
    final BroadcastJoinHelper broadcastJoinHelper;

    if (baseInput.hasInputChannel()) {
      inputChannels.add(baseInput.getInputChannel());
    }

    if (dataSource instanceof JoinDataSource) {
      final Int2IntMap sideStageChannelNumberMap = new Int2IntOpenHashMap();
      final List<FrameReader> channelReaders = new ArrayList<>();

      if (baseInput.hasInputChannel()) {
        // BroadcastJoinHelper doesn't need to read the base channel, so stub in a null reader.
        channelReaders.add(null);
      }

      for (Map.Entry<Integer, ReadableFrameChannel> sideChannelEntry : sideChannels.entrySet()) {
        final int stageNumber = sideChannelEntry.getKey();
        sideStageChannelNumberMap.put(stageNumber, inputChannels.size());
        inputChannels.add(sideChannelEntry.getValue());
        channelReaders.add(sideChannelReaders.get(stageNumber));
      }

      broadcastJoinHelper = new BroadcastJoinHelper(
          sideStageChannelNumberMap,
          inputChannels,
          channelReaders,
          joinableFactory
      );
    } else {
      broadcastJoinHelper = null;
    }

    return Pair.of(inputChannels, broadcastJoinHelper);
  }
}
