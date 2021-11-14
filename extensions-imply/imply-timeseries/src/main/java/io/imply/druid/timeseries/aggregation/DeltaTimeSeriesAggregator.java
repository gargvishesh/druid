/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.DeltaTimeSeries;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class DeltaTimeSeriesAggregator implements Aggregator
{
  private final BaseDoubleColumnValueSelector dataSelector;
  private final BaseLongColumnValueSelector timeSelector;
  private final DeltaTimeSeries deltaTimeSeries;

  public DeltaTimeSeriesAggregator(final BaseLongColumnValueSelector timeSelector,
                                  final BaseDoubleColumnValueSelector dataSelector,
                                  final DurationGranularity durationGranularity,
                                  final Interval window,
                                  final int maxEntries)
  {
    this.dataSelector = dataSelector;
    this.timeSelector = timeSelector;
    this.deltaTimeSeries = new DeltaTimeSeries(durationGranularity, window, maxEntries);
  }

  @Override
  public void aggregate()
  {
    if (dataSelector.isNull() || timeSelector.isNull()) {
      return;
    }
    deltaTimeSeries.addDataPoint(timeSelector.getLong(), dataSelector.getDouble());
  }

  @Nullable
  @Override
  public Object get()
  {
    return deltaTimeSeries;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {

  }
}
