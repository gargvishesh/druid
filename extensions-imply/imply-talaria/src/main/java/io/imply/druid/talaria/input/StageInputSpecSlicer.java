/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import io.imply.druid.talaria.kernel.ReadablePartitions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.List;

/**
 * Slices {@link StageInputSpec} into {@link StageInputSlice}.
 */
public class StageInputSpecSlicer implements InputSpecSlicer
{
  // Stage number -> partitions for that stage
  private final Int2ObjectMap<ReadablePartitions> stagePartitionsMap;

  public StageInputSpecSlicer(final Int2ObjectMap<ReadablePartitions> stagePartitionsMap)
  {
    this.stagePartitionsMap = stagePartitionsMap;
  }

  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return false;
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    final StageInputSpec stageInputSpec = (StageInputSpec) inputSpec;

    final ReadablePartitions stagePartitions = stagePartitionsMap.get(stageInputSpec.getStageNumber());

    if (stagePartitions == null) {
      throw new ISE("No result partitions available for stage [%d]", stageInputSpec.getStageNumber());
    }

    // Decide how many workers to use, and assign inputs.
    final List<ReadablePartitions> workerPartitions = stagePartitions.split(maxNumSlices);
    final List<InputSlice> retVal = new ArrayList<>();

    for (final ReadablePartitions partitions : workerPartitions) {
      retVal.add(new StageInputSlice(stageInputSpec.getStageNumber(), partitions));
    }

    return retVal;
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    throw new UnsupportedOperationException("Cannot sliceDynamic.");
  }
}
