/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.fasterxml.jackson.core.type.TypeReference;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportPublisher;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.StageDefinition;

import javax.annotation.Nullable;

/**
 * Used by {@link QueryDefinitionValidatorTest}.
 */
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
      final TalariaWarningReportPublisher talariaWarningReportPublisher
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeReference getAccumulatedResultTypeReference()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object newAccumulatedResult()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object accumulateResult(Object accumulated, Object current)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object mergeAccumulatedResult(Object accumulated, Object otherAccumulated)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExtraInfoHolder makeExtraInfoHolder(@Nullable Object extra)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int inputFileCount()
  {
    return INPUT_FILES;
  }
}
