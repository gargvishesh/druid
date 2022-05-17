/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportPublisher;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.StageDefinition;

import javax.annotation.Nullable;

public class TestFrameProcessorFactory implements FrameProcessorFactory
{
  private final int INPUT_FILES;

  public TestFrameProcessorFactory(int inputFiles)
  {
    this.INPUT_FILES = inputFiles;
  }

  @Override
  public ProcessorsAndChannels makeProcessors(
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
  )
  {
    return null;
  }

  @Override
  public TypeReference getAccumulatedResultTypeReference()
  {
    return null;
  }

  @Override
  public Object newAccumulatedResult()
  {
    return null;
  }

  @Override
  public Object accumulateResult(Object accumulated, Object current)
  {
    return null;
  }

  @Override
  public Object mergeAccumulatedResult(Object accumulated, Object otherAccumulated)
  {
    return null;
  }

  @Override
  public ExtraInfoHolder makeExtraInfoHolder(@Nullable Object extra)
  {
    return null;
  }

  @Override
  public int inputFileCount()
  {
    return INPUT_FILES;
  }
}
