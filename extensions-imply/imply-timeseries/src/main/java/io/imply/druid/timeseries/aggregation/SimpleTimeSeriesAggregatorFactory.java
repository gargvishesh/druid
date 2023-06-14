/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import io.imply.druid.timeseries.ByteBufferTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;

import javax.annotation.Nullable;


public class SimpleTimeSeriesAggregatorFactory extends BaseTimeSeriesAggregatorFactory
{
  public static final Integer DEFAULT_MAX_ENTRIES = 7200; // 2 hours with per second granularity

  private SimpleTimeSeriesAggregatorFactory(
      String name,
      @Nullable String dataColumn,
      @Nullable String timeColumn,
      @Nullable String timeseriesColumn,
      @Nullable Long timeBucketMillis,
      Interval window,
      int maxEntries
  )
  {
    super(name, dataColumn, timeColumn, timeseriesColumn, timeBucketMillis, window, maxEntries);
  }

  @JsonCreator
  public static SimpleTimeSeriesAggregatorFactory getTimeSeriesAggregationFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("dataColumn") @Nullable final String dataColumn,
      @JsonProperty("timeColumn") @Nullable final String timeColumn,
      @JsonProperty("timeseriesColumn") @Nullable final String timeseriesColumn,
      @JsonProperty("window") Interval window,
      @JsonProperty("maxEntries") @Nullable final Integer maxEntries
  )
  {
    boolean isPrebuilt = timeseriesColumn != null;
    boolean isRawData = dataColumn != null && timeColumn != null;
    if (isPrebuilt == isRawData) {
      throw new IAE("Must exclusively have a valid, non-null (timeColumn, dataColumn) or timeseriesColumn");
    }
    if (window == null) {
      window = Intervals.ETERNITY;
    }
    int finalMaxEntries = DEFAULT_MAX_ENTRIES;
    if (maxEntries != null) {
      finalMaxEntries = maxEntries;
    }
    return new SimpleTimeSeriesAggregatorFactory(
        name,
        dataColumn,
        timeColumn,
        timeseriesColumn,
        null,
        window,
        finalMaxEntries
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.SIMPLE_TIMESERIES_CACHE_ID);
    addCacheKeys(builder);
    return builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      return new SimpleTimeSeriesMergeAggregator(
          metricFactory.makeColumnValueSelector(getTimeseriesColumn()), window, maxEntries
      );
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new SimpleTimeSeriesBuildAggregator(timeSelector, dataSelector, getWindow(), getMaxEntries());
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector = metricFactory.makeColumnValueSelector(
          getTimeseriesColumn());
      return new SimpleTimeSeriesMergeBufferAggregator(
          selector,
          getWindow(),
          getMaxEntries()
      );
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new SimpleTimeSeriesBuildBufferAggregator(
          timeSelector,
          dataSelector,
          getWindow(),
          getMaxEntries()
      );
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    SimpleTimeSeriesContainer leftSeries = (SimpleTimeSeriesContainer) lhs;
    SimpleTimeSeriesContainer rightSeries = (SimpleTimeSeriesContainer) rhs;

    if (leftSeries == null || leftSeries.isNull()) {
      return rightSeries;
    }

    if (rightSeries == null || rightSeries.isNull()) {
      return leftSeries;
    }

    rightSeries.pushInto(leftSeries);
    return leftSeries;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SimpleTimeSeriesAggregatorFactory(
        getName(),
        null,
        null,
        getName(),
        getTimeBucketMillis(),
        getWindow(),
        getMaxEntries()
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    return SimpleTimeSeriesContainer.createFromObject(object, getWindow(), getMaxEntries());
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return BaseTimeSeriesAggregatorFactory.TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return maxEntries * (Long.BYTES + Double.BYTES) + ByteBufferTimeSeries.DATA_OFFSET;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new SimpleTimeSeriesAggregatorFactory(
        newName,
        getDataColumn(),
        getTimeColumn(),
        getTimeseriesColumn(),
        getTimeBucketMillis(),
        getWindow(),
        getMaxEntries()
    );
  }
}
