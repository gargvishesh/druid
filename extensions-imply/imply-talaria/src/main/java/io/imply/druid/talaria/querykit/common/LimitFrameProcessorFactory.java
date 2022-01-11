/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.talaria.frame.channel.ReadableConcatFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.querykit.BaseFrameProcessorFactory;
import io.imply.druid.talaria.util.SupplierIterator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;

@JsonTypeName("limit")
public class LimitFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private final long limit;

  @JsonCreator
  public LimitFrameProcessorFactory(@JsonProperty("limit") final long limit)
  {
    this.limit = limit;
  }

  @JsonProperty
  public long getLimit()
  {
    return limit;
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
    if (workerNumber > 0) {
      // We use a simplistic limiting approach: funnel all data through a single worker and write it to a
      // single output partition. So limiting stages must have a single worker.
      throw new ISE("%s must be configured with maxWorkerCount = 1", getClass().getSimpleName());
    }

    final int numInputChannels = inputChannels.getStagePartitions().size();

    if (numInputChannels == 0) {
      return new ProcessorsAndChannels<>(Sequences.empty(), OutputChannels.none());
    }

    final OutputChannel outputChannel = outputChannelFactory.openChannel(0, false);

    final Supplier<FrameProcessor<Long>> workerSupplier = () -> {
      // TODO(gianm): assumes all input channels have the same frame signature. validate?
      final FrameReader frameReader =
          inputChannels.getFrameReader(inputChannels.getStagePartitions().iterator().next());

      final Iterator<ReadableFrameChannel> channelIterator =
          IntStream.range(0, numInputChannels).mapToObj(
              i -> {
                try {
                  return inputChannels.openChannel(inputChannels.getStagePartitions().get(i));
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
          ).iterator();

      return new LimitFrameProcessor(
          ReadableConcatFrameChannel.open(channelIterator),
          outputChannel.getWritableChannel(),
          frameReader,
          limit
      );
    };

    final Sequence<FrameProcessor<Long>> processors =
        Sequences.simple(() -> new SupplierIterator<>(workerSupplier));

    return new ProcessorsAndChannels<>(processors, OutputChannels.wrap(Collections.singletonList(outputChannel)));
  }
}
