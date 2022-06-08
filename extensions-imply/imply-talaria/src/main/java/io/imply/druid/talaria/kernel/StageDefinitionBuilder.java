/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import org.apache.druid.segment.column.RowSignature;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StageDefinitionBuilder
{
  private final int stageNumber;
  private int[] inputStageNumbers = new int[]{};
  private int[] broadcastInputStageNumbers = new int[]{};
  @SuppressWarnings("rawtypes")
  private FrameProcessorFactory processorFactory;
  private RowSignature signature = RowSignature.empty();
  private int maxWorkerCount = 1;
  private ShuffleSpec shuffleSpec = null;
  private boolean shuffleCheckHasMultipleValues = false;

  /**
   * Callers should use {@link StageDefinition#builder(int)} instead of directly using this constructor
   */
  StageDefinitionBuilder(final int stageNumber)
  {
    this.stageNumber = stageNumber;
  }

  public StageDefinitionBuilder inputStages(final int... inputStageNumbers)
  {
    this.inputStageNumbers = inputStageNumbers;
    return this;
  }

  public StageDefinitionBuilder broadcastInputStages(final int... broadcastInputStageNumbers)
  {
    this.broadcastInputStageNumbers = broadcastInputStageNumbers;
    return this;
  }

  @SuppressWarnings("rawtypes")
  public StageDefinitionBuilder processorFactory(final FrameProcessorFactory processorFactory)
  {
    this.processorFactory = processorFactory;
    return this;
  }

  public StageDefinitionBuilder signature(final RowSignature signature)
  {
    this.signature = signature;
    return this;
  }

  public StageDefinitionBuilder maxWorkerCount(final int maxWorkerCount)
  {
    this.maxWorkerCount = maxWorkerCount;
    return this;
  }

  public StageDefinitionBuilder shuffleCheckHasMultipleValues(final boolean shuffleCheckHasMultipleValues)
  {
    this.shuffleCheckHasMultipleValues = shuffleCheckHasMultipleValues;
    return this;
  }

  public StageDefinitionBuilder shuffleSpec(final ShuffleSpec shuffleSpec)
  {
    this.shuffleSpec = shuffleSpec;
    return this;
  }

  int getStageNumber()
  {
    return stageNumber;
  }

  int[] getInputStageNumbers()
  {
    return inputStageNumbers;
  }

  public StageDefinition build(final String queryId)
  {
    return new StageDefinition(
        new StageId(queryId, stageNumber),
        Arrays.stream(inputStageNumbers)
              .mapToObj(stageNumber -> new StageId(queryId, stageNumber))
              .collect(Collectors.toList()),
        Arrays.stream(broadcastInputStageNumbers)
              .mapToObj(stageNumber -> new StageId(queryId, stageNumber))
              .collect(Collectors.toSet()),
        processorFactory,
        signature,
        shuffleSpec,
        maxWorkerCount,
        shuffleCheckHasMultipleValues
    );
  }
}
