/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.imply.druid.talaria.exec.Limits;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSpec;
import io.imply.druid.talaria.input.InputSpecSlicer;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;
import java.util.OptionalInt;

public enum WorkerAssignmentStrategy
{
  /**
   * Use the highest possible number of tasks, while staying within {@link StageDefinition#getMaxWorkerCount()}.
   */
  MAX {
    @Override
    public List<InputSlice> assign(
        StageDefinition stageDef,
        InputSpec inputSpec,
        Int2IntMap stageWorkerCountMap,
        InputSpecSlicer slicer
    )
    {
      return slicer.sliceStatic(inputSpec, stageDef.getMaxWorkerCount());
    }
  },

  /**
   * Use the lowest possible number of tasks, while keeping each task's workload under
   * {@link Limits#MAX_INPUT_FILES_PER_WORKER} files and {@link Limits#MAX_INPUT_BYTES_PER_WORKER} bytes.
   */
  AUTO {
    @Override
    public List<InputSlice> assign(
        final StageDefinition stageDef,
        final InputSpec inputSpec,
        final Int2IntMap stageWorkerCountMap,
        final InputSpecSlicer slicer
    )
    {
      if (slicer.canSliceDynamic(inputSpec)) {
        return slicer.sliceDynamic(
            inputSpec,
            stageDef.getMaxWorkerCount(),
            Limits.MAX_INPUT_FILES_PER_WORKER,
            Limits.MAX_INPUT_BYTES_PER_WORKER
        );
      } else {
        // In auto mode, if we can't slice inputs dynamically, we instead carry forwards the number of workers from
        // the prior stages (or use 1 worker if there are no input stages).

        // To handle cases where the input stage is limited to 1 worker because it is reading 1 giant file, I think it
        // would be better to base the number of workers on the number of rows read by the prior stage, which would
        // allow later stages to fan out when appropriate. However, we're not currently tracking this information
        // in a way that is accessible to the assignment strategy.

        final IntSet inputStages = stageDef.getInputStageNumbers();
        final OptionalInt maxInputStageWorkerCount = inputStages.intStream().map(stageWorkerCountMap).max();
        final int workerCount = maxInputStageWorkerCount.orElse(1);
        return slicer.sliceStatic(inputSpec, workerCount);
      }
    }
  };

  @JsonCreator
  public static WorkerAssignmentStrategy fromString(final String name)
  {
    if (name == null) {
      throw new NullPointerException("Null worker assignment strategy");
    }
    return valueOf(StringUtils.toUpperCase(name));
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toLowerCase(name());
  }

  public abstract List<InputSlice> assign(
      StageDefinition stageDef,
      InputSpec inputSpec,
      Int2IntMap stageWorkerCountMap,
      InputSpecSlicer slicer
  );
}
