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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesComplexMetricSerde;
import io.imply.druid.timeseries.ByteBufferTimeSeries;
import io.imply.druid.timeseries.SerdeUtils;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.aggregation.postprocessors.TimeSeriesFn;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.NonnullPair;
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
import java.util.List;
import java.util.Map;


public class SimpleTimeSeriesAggregatorFactory extends BaseTimeSeriesAggregatorFactory
{
  public static final Integer DEFAULT_MAX_ENTRIES = 7200; // 2 hours with per second granularity

  private SimpleTimeSeriesAggregatorFactory(
      String name,
      @Nullable String dataColumn,
      @Nullable String timeColumn,
      @Nullable String timeseriesColumn,
      @Nullable List<TimeSeriesFn> postProcessing,
      @Nullable Long timeBucketMillis,
      Interval window,
      int maxEntries
  )
  {
    super(name, dataColumn, timeColumn, timeseriesColumn, postProcessing, timeBucketMillis, window, maxEntries);
  }

  @JsonCreator
  public static SimpleTimeSeriesAggregatorFactory getTimeSeriesAggregationFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("dataColumn") @Nullable final String dataColumn,
      @JsonProperty("timeColumn") @Nullable final String timeColumn,
      @JsonProperty("timeseriesColumn") @Nullable final String timeseriesColumn,
      @JsonProperty("postProcessing") @Nullable final List<TimeSeriesFn> postProcessing,
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
      window = SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW;
    }
    int finalMaxEntries = DEFAULT_MAX_ENTRIES;
    if (maxEntries != null) {
      finalMaxEntries = maxEntries;
    }
    return new SimpleTimeSeriesAggregatorFactory(name,
                                                 dataColumn,
                                                 timeColumn,
                                                 timeseriesColumn,
                                                 postProcessing,
                                                 null,
                                                 window,
                                                 finalMaxEntries);
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
      return new SimpleTimeSeriesAggregator(timeSelector, dataSelector, getwindow(), getMaxEntries());
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      BaseObjectColumnValueSelector<SimpleTimeSeries> selector = metricFactory.makeColumnValueSelector(
          getTimeseriesColumn());
      return new SimpleTimeSeriesMergeBufferAggregator(
          selector,
          getwindow(),
          getMaxEntries()
      );
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new SimpleTimeSeriesBuildBufferAggregator(
          timeSelector,
          dataSelector,
          getwindow(),
          getMaxEntries()
      );
    }
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    SimpleTimeSeries leftSeries = (SimpleTimeSeries) lhs;
    SimpleTimeSeries rightSeries = (SimpleTimeSeries) rhs;
    if (rightSeries != null && leftSeries != null) {
      leftSeries.addTimeSeries(rightSeries);
    } else if (rightSeries != null) {
      return rightSeries;
    }
    return leftSeries;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SimpleTimeSeriesAggregatorFactory(getName(),
                                                 null,
                                                 null,
                                                 getName(),
                                                 getPostProcessing(),
                                                 getTimeBucketMillis(),
                                                 getwindow(),
                                                 getMaxEntries());
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof SimpleTimeSeries) {
      return object;
    }
    Map<String, Object> timeseriesKeys = (Map<String, Object>) object;
    ImplyLongArrayList timestamps = new ImplyLongArrayList((List<Long>) timeseriesKeys.get("timestamps"));
    ImplyDoubleArrayList dataPoints = new ImplyDoubleArrayList((List<Double>) timeseriesKeys.get("dataPoints"));
    NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> bounds = SerdeUtils.getBounds(timeseriesKeys);
    return new SimpleTimeSeries(timestamps, dataPoints, getwindow(), bounds.lhs, bounds.rhs, getMaxEntries());
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object == null) {
      return null;
    }

    SimpleTimeSeries finalResult = (SimpleTimeSeries) object;
    finalResult.build();
    return super.finalizeComputation(finalResult);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.ofComplex("imply-ts-simple");
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
        getPostProcessing(),
        getTimeBucketMillis(),
        getwindow(),
        getMaxEntries()
    );
  }
}
