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
import io.imply.druid.talaria.input.InputSpec;
import io.imply.druid.talaria.input.InputSpecs;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StageDefinitionBuilder
{
  private final int stageNumber;
  private final List<InputSpec> inputSpecs = new ArrayList<>();
  private final IntSet broadcastInputNumbers = new IntRBTreeSet();
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

  public StageDefinitionBuilder inputs(final List<InputSpec> inputSpecs)
  {
    this.inputSpecs.clear();
    this.inputSpecs.addAll(inputSpecs);
    return this;
  }

  public StageDefinitionBuilder inputs(final InputSpec... inputSpecs)
  {
    return inputs(Arrays.asList(inputSpecs));
  }

  public StageDefinitionBuilder broadcastInputs(final IntSet broadcastInputNumbers)
  {
    this.broadcastInputNumbers.clear();

    for (int broadcastInputNumber : broadcastInputNumbers) {
      this.broadcastInputNumbers.add(broadcastInputNumber);
    }

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

  IntSet getInputStageNumbers()
  {
    return InputSpecs.getStageNumbers(inputSpecs);
  }

  public StageDefinition build(final String queryId)
  {
    return new StageDefinition(
        new StageId(queryId, stageNumber),
        inputSpecs,
        broadcastInputNumbers,
        processorFactory,
        signature,
        shuffleSpec,
        maxWorkerCount,
        shuffleCheckHasMultipleValues
    );
  }
}
