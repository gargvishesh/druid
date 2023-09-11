/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.DownsampledSumTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class DownsampledSumTimeSeriesMergeAggregator implements Aggregator
{
  private DownsampledSumTimeSeries timeSeries;
  private final BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector;
  private final Interval window;
  private final DurationGranularity timeBucketGranularity;
  private final int maxEntries;

  public DownsampledSumTimeSeriesMergeAggregator(
      BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector,
      DurationGranularity timeBucketGranularity,
      Interval window,
      int maxEntries
  )
  {
    this.selector = selector;
    this.timeBucketGranularity = timeBucketGranularity;
    this.window = window;
    this.maxEntries = maxEntries;
  }

  @Override
  public void aggregate()
  {
    SimpleTimeSeriesContainer timeSeriesContainer = selector.getObject();

    if (timeSeriesContainer == null) {
      return;
    }

    if (timeSeries == null) {
      timeSeries = new DownsampledSumTimeSeries(timeBucketGranularity, window, maxEntries);
    }
    timeSeries.addSimpleTimeSeries(timeSeriesContainer.computeSimple());
  }

  @Nullable
  @Override
  public Object get()
  {
    if (timeSeries == null) {
      return SimpleTimeSeriesContainer.createFromInstance(null);
    }
    return SimpleTimeSeriesContainer.createFromInstance(timeSeries.computeSimple());
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
