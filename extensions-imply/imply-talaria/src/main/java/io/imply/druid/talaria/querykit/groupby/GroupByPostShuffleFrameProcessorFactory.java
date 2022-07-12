/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSliceReader;
import io.imply.druid.talaria.input.ReadableInput;
import io.imply.druid.talaria.input.StageInputSlice;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.querykit.BaseFrameProcessorFactory;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

@JsonTypeName("groupByPostShuffle")
public class GroupByPostShuffleFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private final GroupByQuery query;

  @JsonCreator
  public GroupByPostShuffleFrameProcessorFactory(
      @JsonProperty("query") GroupByQuery query
  )
  {
    this.query = query;
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @Override
  public ProcessorsAndChannels<FrameProcessor<Long>, Long> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  )
  {
    // Expecting a single input slice from some prior stage.
    final StageInputSlice slice = (StageInputSlice) Iterables.getOnlyElement(inputSlices);
    final GroupByStrategySelector strategySelector = frameContext.groupByStrategySelector();

    final Int2ObjectMap<OutputChannel> outputChannels = new Int2ObjectOpenHashMap<>();
    for (final ReadablePartition partition : slice.getPartitions()) {
      outputChannels.computeIfAbsent(
          partition.getPartitionNumber(),
          i -> {
            try {
              return outputChannelFactory.openChannel(i);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    final Sequence<ReadableInput> readableInputs =
        Sequences.simple(inputSliceReader.attach(0, slice, counters, warningPublisher));

    final Sequence<FrameProcessor<Long>> processors = readableInputs.map(
        readableInput -> {
          final OutputChannel outputChannel =
              outputChannels.get(readableInput.getStagePartition().getPartitionNumber());

          return new GroupByPostShuffleFrameProcessor(
              query,
              strategySelector,
              readableInput.getChannel(),
              outputChannel.getWritableChannel(),
              readableInput.getChannelFrameReader(),
              stageDefinition.getSignature(),
              stageDefinition.getClusterBy(),
              outputChannel.getFrameMemoryAllocator()
          );
        }
    );

    return new ProcessorsAndChannels<>(
        processors,
        OutputChannels.wrapReadOnly(ImmutableList.copyOf(outputChannels.values()))
    );
  }
}
