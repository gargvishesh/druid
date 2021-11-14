/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

public class TimeSeriesAggregationConfigTest
{
  @Test
  public void testMaxEntriesMeanTimeSeries()
  {
    MeanTimeSeriesAggregatorFactory meanTimeSeriesAggregationFactory =
        MeanTimeSeriesAggregatorFactory.getMeanTimeSeriesAggregationFactory("",
                                                                            "",
                                                                            "",
                                                                            null,
                                                                            null,
                                                                            2L,
                                                                            Intervals.utc(3, 7),
                                                                            null);
    Assert.assertEquals(3, meanTimeSeriesAggregationFactory.getMaxEntries());
  }

  @Test
  public void testMaxEntriesDeltaTimeSeries()
  {
    DeltaTimeSeriesAggregatorFactory deltaTimeSeriesAggregatorFactory =
        DeltaTimeSeriesAggregatorFactory.getDeltaTimeSeriesAggregationFactory("",
                                                                              "",
                                                                              "",
                                                                              null,
                                                                              null,
                                                                              2L,
                                                                              Intervals.utc(3, 7),
                                                                              null);
    Assert.assertEquals(3, deltaTimeSeriesAggregatorFactory.getMaxEntries());
  }
}
