/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSliceReader;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.StageDefinition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface FrameProcessorFactory<ExtraInfoType, ProcessorType extends FrameProcessor<T>, T, R>
{
  /**
   * Create processors for a particular worker in a particular stage. The processors will be run on a thread pool,
   * with at most "maxOutstandingProcessors" number of processors outstanding at once.
   *
   * The iterator returned by {@link ProcessorsAndChannels#processors()} will be passed directly to
   * {@link FrameProcessors#runAllFully}. This method therefore inherits the promise
   * from runAllFully that the iterator will be manipulated (i.e., have hasNext and next called on it) just-in-time, in
   * a critical section, immediately before each processor starts.
   *
   * @param stageDefinition          stage definition
   * @param workerNumber             current worker number; some factories use this to determine what work to do
   * @param inputSlices              input slices for this worker, indexed by input number (one for each
   *                                 {@link StageDefinition#getInputSpecs()})
   * @param inputSliceReader         reader for the input slices
   * @param extra                    any extra, out-of-band information associated with this particular worker; some
   *                                 factories use this to determine what work to do
   * @param outputChannelFactory     factory for generating output channels.
   * @param frameContext             Context which provides services needed by frame processors
   * @param maxOutstandingProcessors maximum number of processors that will be active at once
   * @param counters                 allows creation of custom processor counters
   * @param warningPublisher         publisher for warnings encountered during execution
   *
   * @return a processor iterator, which may be computed lazily; and a list of output channels.
   */
  ProcessorsAndChannels<ProcessorType, T> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable ExtraInfoType extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  ) throws IOException;

  TypeReference<R> getAccumulatedResultTypeReference();

  R newAccumulatedResult();

  // TODO(gianm): Javadoc says "accumulated" may be modified and returned (or not)
  R accumulateResult(R accumulated, T current);

  // TODO(gianm): Javadoc says "accumulated" may be modified and returned (or not)
  R mergeAccumulatedResult(R accumulated, R otherAccumulated);

  @SuppressWarnings("rawtypes")
  ExtraInfoHolder makeExtraInfoHolder(@Nullable ExtraInfoType extra);
}
