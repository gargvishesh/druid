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
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.StageDefinition;

import javax.annotation.Nullable;
import java.io.IOException;

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
   * @param workerNumber             current worker number; some factories use this to determine what work to do
   * @param extra                    any extra, out-of-band information associated with this particular worker; some
   *                                 factories use this to determine what work to do
   * @param inputChannels            provider for input channels.
   * @param outputChannelFactory     factory for generating output channels.
   * @param stageDefinition          stage definition
   * @param clusterBy                represents the expected ordering of frames written to output channels. If the
   *                                 input is not already sorted this way, then each frame must be sorted using
   *                                 {@link io.imply.druid.talaria.frame.write.FrameWriter#sort} before writing. It is not
   *                                 necessary to do any sorting of data across frames; it is only required that each
   *                                 individual frame is internally sorted.
   * @param providerThingy           Context which provides services needed by frame processors
   * @param maxOutstandingProcessors maximum number of processors that will be active at once
   *
   * @return a processor iterator, which may be computed lazily; and a list of output channels.
   */
  ProcessorsAndChannels<ProcessorType, T> makeProcessors(
      int workerNumber,
      @Nullable ExtraInfoType extra,
      InputChannels inputChannels,
      OutputChannelFactory outputChannelFactory,
      StageDefinition stageDefinition,
      ClusterBy clusterBy,
      FrameContext providerThingy,
      int maxOutstandingProcessors,
      TalariaCounters talariaCounters
  ) throws IOException;

  TypeReference<R> getAccumulatedResultTypeReference();

  R newAccumulatedResult();

  // TODO(gianm): Javadoc says "accumulated" may be modified and returned (or not)
  R accumulateResult(R accumulated, T current);

  // TODO(gianm): Javadoc says "accumulated" may be modified and returned (or not)
  R mergeAccumulatedResult(R accumulated, R otherAccumulated);

  @SuppressWarnings("rawtypes")
  ExtraInfoHolder makeExtraInfoHolder(@Nullable ExtraInfoType extra);

  /**
   * @return total count of the input files.
   */
  default int inputFilesCount()
  {
    return 0;
  }
}
