/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.read.FrameComparisonWidgetImpl;
import io.imply.druid.talaria.frame.read.FrameReaderUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Comparator;
import java.util.List;

public class ClusterByKeyComparator implements Comparator<ClusterByKey>
{
  private final int firstFieldPosition;
  private final int[] ascDescRunLengths;

  private ClusterByKeyComparator(
      final int firstFieldPosition,
      final int[] ascDescRunLengths
  )
  {
    this.firstFieldPosition = firstFieldPosition;
    this.ascDescRunLengths = ascDescRunLengths;
  }

  /**
   * Use {@link ClusterBy#keyComparator()}.
   */
  static ClusterByKeyComparator create(final List<ClusterByColumn> keyColumns)
  {
    return new ClusterByKeyComparator(
        computeFirstFieldPosition(keyColumns.size()),
        computeAscDescRunLengths(keyColumns)
    );
  }

  /**
   * Compute the offset into each key where the first field starts.
   *
   * Public so {@link FrameComparisonWidgetImpl} can use it.
   */
  public static int computeFirstFieldPosition(final int fieldCount)
  {
    return Ints.checkedCast((long) fieldCount * Integer.BYTES);
  }

  /**
   * Given a list of sort columns, compute an array of the number of ascending fields in a run, followed by number of
   * descending fields in a run, followed by ascending, etc. For example: ASC, ASC, DESC, ASC would return [2, 1, 1]
   * and DESC, DESC, ASC would return [0, 2, 1].
   *
   * Public so {@link FrameComparisonWidgetImpl} can use it.
   */
  public static int[] computeAscDescRunLengths(final List<ClusterByColumn> sortColumns)
  {
    final IntList ascDescRunLengths = new IntArrayList(4);

    boolean descending = false;
    int runLength = 0;

    for (final ClusterByColumn column : sortColumns) {
      if (column.descending() != descending) {
        ascDescRunLengths.add(runLength);
        runLength = 0;
        descending = !descending;
      }

      runLength++;
    }

    if (runLength > 0) {
      ascDescRunLengths.add(runLength);
    }

    return ascDescRunLengths.toIntArray();
  }

  @Override
  @SuppressWarnings("SubtractionInCompareTo")
  public int compare(final ClusterByKey key1, final ClusterByKey key2)
  {
    // Similar logic to FrameComparaisonWidgetImpl, but implementation is different enough that we need our own.
    // Major difference is Frame v. Frame instead of byte[] v. byte[].

    final byte[] keyArray1 = key1.array();
    final byte[] keyArray2 = key2.array();

    int comparableBytesStartPosition1 = firstFieldPosition;
    int comparableBytesStartPosition2 = firstFieldPosition;

    boolean ascending = true;
    int field = 0;

    for (int numFields : ascDescRunLengths) {
      if (numFields > 0) {
        final int nextField = field + numFields;
        final int comparableBytesEndPosition1 = ClusterByKeyReader.fieldEndPosition(keyArray1, nextField - 1);
        final int comparableBytesEndPosition2 = ClusterByKeyReader.fieldEndPosition(keyArray2, nextField - 1);

        int cmp = FrameReaderUtils.compareByteArraysUnsigned(
            keyArray1,
            comparableBytesStartPosition1,
            comparableBytesEndPosition1 - comparableBytesStartPosition1,
            keyArray2,
            comparableBytesStartPosition2,
            comparableBytesEndPosition2 - comparableBytesStartPosition2
        );

        if (cmp != 0) {
          return ascending ? cmp : -cmp;
        }

        field = nextField;
        comparableBytesStartPosition1 = comparableBytesEndPosition1;
        comparableBytesStartPosition2 = comparableBytesEndPosition2;
      }

      ascending = !ascending;
    }

    return 0;
  }
}
