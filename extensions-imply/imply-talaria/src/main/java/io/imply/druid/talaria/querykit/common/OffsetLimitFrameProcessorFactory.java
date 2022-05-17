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
import com.fasterxml.jackson.annotation.JsonInclude;
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
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportPublisher;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.querykit.BaseFrameProcessorFactory;
import io.imply.druid.talaria.util.SupplierIterator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;

@JsonTypeName("limit")
public class OffsetLimitFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private final long offset;

  @Nullable
  private final Long limit;

  @JsonCreator
  public OffsetLimitFrameProcessorFactory(
      @JsonProperty("offset") final long offset,
      @Nullable @JsonProperty("limit") final Long limit
  )
  {
    this.offset = offset;
    this.limit = limit;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getOffset()
  {
    return offset;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getLimit()
  {
    return limit;
  }

  @Override
  public ProcessorsAndChannels<FrameProcessor<Long>, Long> makeProcessors(
      int workerNumber,
      @Nullable Object extra,
      InputChannels inputChannels,
      OutputChannelFactory outputChannelFactory,
      StageDefinition stageDefinition,
      ClusterBy clusterBy,
      FrameContext providerThingy,
      int maxOutstandingProcessors,
      TalariaCounters talariaCounters,
      TalariaWarningReportPublisher talariaWarningReportPublisher
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

    final OutputChannel outputChannel = outputChannelFactory.openChannel(0);

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

      // Note: OffsetLimitFrameProcessor does not use allocator from the outputChannel; it uses unlimited instead.
      return new OffsetLimitFrameProcessor(
          ReadableConcatFrameChannel.open(channelIterator),
          outputChannel.getWritableChannel(),
          frameReader,
          offset,
          // Limit processor will add limit + offset at various points; must avoid overflow
          limit == null ? Long.MAX_VALUE - offset : limit
      );
    };

    final Sequence<FrameProcessor<Long>> processors =
        Sequences.simple(() -> new SupplierIterator<>(workerSupplier));

    return new ProcessorsAndChannels<>(
        processors,
        OutputChannels.wrapReadOnly(Collections.singletonList(outputChannel))
    );
  }
}
