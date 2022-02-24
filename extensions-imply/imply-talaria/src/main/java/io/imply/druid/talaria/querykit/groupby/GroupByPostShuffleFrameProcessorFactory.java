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
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.kernel.StagePartition;
import io.imply.druid.talaria.querykit.BaseFrameProcessorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

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
      int workerNumber,
      @Nullable Object extra,
      InputChannels inputChannels,
      OutputChannelFactory outputChannelFactory,
      RowSignature signature,
      ClusterBy clusterBy,
      FrameContext providerThingy,
      int maxOutstandingProcessors
  ) throws IOException
  {
    final GroupByStrategySelector strategySelector = providerThingy.groupByStrategySelector();

    final List<OutputChannel> outputChannels = new ArrayList<>();
    for (final StagePartition partition : inputChannels.getStagePartitions()) {
      outputChannels.add(outputChannelFactory.openChannel(partition.getPartitionNumber()));
    }

    final Sequence<Integer> inputSequence =
        Sequences.simple(() -> IntStream.range(0, inputChannels.getStagePartitions().size()).iterator());

    final Sequence<FrameProcessor<Long>> processors = inputSequence.map(
        i -> {
          try {
            final StagePartition stagePartition = inputChannels.getStagePartitions().get(i);

            return new GroupByPostShuffleFrameProcessor(
                query,
                strategySelector,
                inputChannels.openChannel(stagePartition),
                outputChannels.get(i).getWritableChannel(),
                inputChannels.getFrameReader(stagePartition),
                signature,
                outputChannels.get(i).getFrameMemoryAllocator()
            );
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );

    return new ProcessorsAndChannels<>(processors, OutputChannels.wrap(outputChannels));
  }
}
