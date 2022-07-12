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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.frame.channel.ReadableConcatFrameChannel;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSliceReader;
import io.imply.druid.talaria.input.ReadableInput;
import io.imply.druid.talaria.input.ReadableInputs;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.querykit.BaseFrameProcessorFactory;
import io.imply.druid.talaria.util.SupplierIterator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
  ) throws IOException
  {
    if (workerNumber > 0) {
      // We use a simplistic limiting approach: funnel all data through a single worker, single processor, and
      // single output partition. So limiting stages must have a single worker.
      throw new ISE("%s must be configured with maxWorkerCount = 1", getClass().getSimpleName());
    }

    // Expect a single input slice.
    final InputSlice slice = Iterables.getOnlyElement(inputSlices);

    if (inputSliceReader.numReadableInputs(slice) == 0) {
      return new ProcessorsAndChannels<>(Sequences.empty(), OutputChannels.none());
    }

    final OutputChannel outputChannel = outputChannelFactory.openChannel(0);

    final Supplier<FrameProcessor<Long>> workerSupplier = () -> {
      final ReadableInputs readableInputs = inputSliceReader.attach(0, slice, counters, warningPublisher);

      if (!readableInputs.hasChannels()) {
        throw new ISE("Processor inputs must be channels");
      }

      // Note: OffsetLimitFrameProcessor does not use allocator from the outputChannel; it uses unlimited instead.
      return new OffsetLimitFrameProcessor(
          ReadableConcatFrameChannel.open(Iterators.transform(readableInputs.iterator(), ReadableInput::getChannel)),
          outputChannel.getWritableChannel(),
          readableInputs.frameReader(),
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
