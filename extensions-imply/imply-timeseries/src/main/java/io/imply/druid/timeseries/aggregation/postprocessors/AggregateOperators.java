/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation.postprocessors;

import org.apache.druid.java.util.common.ISE;

public class AggregateOperators
{
  public static void addIdenticalTimestamps(
      long[] fromTS,
      double[] fromDP,
      long[] toTS,
      double[] toDP,
      long fromSize,
      long toSize
  )
  {
    if (fromSize != toSize) {
      throw new ISE(
          "Addition requires all combining time series "
          + "to have same number of datapoints : [%d, %d]",
          fromSize,
          toSize
      );
    }
    for (int i = 0; i < fromSize; i++) {
      if (fromTS[i] != toTS[i]) {
        throw new ISE(
            "Addition requires all combining time series "
            + "to have same time values : [%d, %d]. Timestamps differ at index : %d",
            fromTS[i],
            toTS[i],
            i
        );
      }
    }
    for (int i = 0; i < fromSize; i++) {
      toDP[i] = fromDP[i] + toDP[i];
    }
  }
}
