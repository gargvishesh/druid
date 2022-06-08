/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.read.FrameComparisonWidget;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;

import java.util.Arrays;
import java.util.List;

/**
 * Utility for sorting frames in-place.
 */
public class FrameSort
{
  private FrameSort()
  {
    // No instantiation.
  }

  /**
   * Sort the given frame in-place. It must be a permuted frame.
   *
   * @param frame            frame to sort
   * @param frameReader      frame reader
   * @param clusterByColumns list of columns to sort by; must be nonempty.
   */
  public static void sort(
      final Frame frame,
      final FrameReader frameReader,
      final List<ClusterByColumn> clusterByColumns
  )
  {
    if (!frame.isPermuted()) {
      throw new ISE("Cannot sort nonpermuted frame");
    }

    if (clusterByColumns.isEmpty()) {
      throw new ISE("Cannot sort with an empty column list");
    }

    final WritableMemory memory = frame.writableMemory();

    // Object array instead of int array, since we want timsort from Arrays.sort. (It does fewer comparisons
    // on partially-sorted data, which is common.)
    final Integer[] rows = new Integer[frame.numRows()];
    for (int i = 0; i < frame.numRows(); i++) {
      rows[i] = i;
    }

    final FrameComparisonWidget comparisonWidget1 = frameReader.makeComparisonWidget(frame, clusterByColumns);
    final FrameComparisonWidget comparisonWidget2 = frameReader.makeComparisonWidget(frame, clusterByColumns);

    Arrays.sort(
        rows,
        (k1, k2) -> comparisonWidget1.compare(k1, comparisonWidget2, k2)
    );

    // Replace logical row numbers with physical row numbers.
    for (int i = 0; i < frame.numRows(); i++) {
      rows[i] = frame.physicalRow(rows[i]);
    }

    // And copy them into the frame.
    for (int i = 0; i < frame.numRows(); i++) {
      memory.putInt(Frame.HEADER_SIZE + (long) i * Integer.BYTES, rows[i]);
    }
  }
}
