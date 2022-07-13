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
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.List;

public class InputSlices
{
  private InputSlices()
  {
    // No instantiation.
  }

  public static ReadablePartitions allReadablePartitions(final List<InputSlice> slices)
  {
    final List<ReadablePartitions> partitionsList = new ArrayList<>();

    for (final InputSlice slice : slices) {
      if (slice instanceof StageInputSlice) {
        partitionsList.add(((StageInputSlice) slice).getPartitions());
      }
    }

    return ReadablePartitions.combine(partitionsList);
  }

  public static int getNumNonBroadcastReadableInputs(
      final List<InputSlice> slices,
      final InputSliceReader reader,
      final IntSet broadcastInputs
  )
  {
    int numInputs = 0;

    for (int i = 0; i < slices.size(); i++) {
      if (!broadcastInputs.contains(i)) {
        numInputs += reader.numReadableInputs(slices.get(i));
      }
    }

    return numInputs;
  }
}
