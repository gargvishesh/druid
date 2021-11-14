/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;

import static io.imply.druid.timeseries.SimpleByteBufferTimeSeriesTest.VISIBLE_WINDOW;
import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public class Util
{
  public static SimpleTimeSeries makeSimpleTS(long[] ts, double[] dp)
  {
    return new SimpleTimeSeries(new ImplyLongArrayList(ts),
                                new ImplyDoubleArrayList(dp),
                                VISIBLE_WINDOW,
                                MAX_ENTRIES);
  }
}
