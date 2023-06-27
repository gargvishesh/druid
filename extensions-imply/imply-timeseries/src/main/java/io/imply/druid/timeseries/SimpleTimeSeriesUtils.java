/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import org.apache.druid.java.util.common.ISE;

public class SimpleTimeSeriesUtils
{
  public static void checkMatchingWindows(SimpleTimeSeries left, SimpleTimeSeries right, String name)
  {
    if (!left.getWindow().equals(right.getWindow())) {
      throw new ISE(
          "%s expects the windows of input time series to be same, but found [%s, %s]",
          name,
          left.getWindow(),
          right.getWindow()
      );
    }
  }

  public static void checkMatchingSizeAndTimestamps(SimpleTimeSeries left, SimpleTimeSeries right, String name)
  {
    if (left.size() != right.size()) {
      throw new ISE(
          "%s requires all combining time series "
          + "to have same number of datapoints. Found the sizes as : [%d, %d]",
          name,
          left.size(),
          right.size()
      );
    }
    for (int i = 0; i < left.size(); i++) {
      if (left.getTimestamps().getLong(i) != right.getTimestamps().getLong(i)) {
        throw new ISE(
            "%s requires all combining time series "
            + "to have same time values : [%d, %d]. Timestamps differ at index : [%d]",
            name,
            left.getTimestamps().getLong(i),
            right.getTimestamps().getLong(i),
            i
        );
      }
    }
  }
}
