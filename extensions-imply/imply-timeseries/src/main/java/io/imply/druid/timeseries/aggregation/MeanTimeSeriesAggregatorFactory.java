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
import io.imply.druid.timeseries.MeanTimeSeries;
import io.imply.druid.timeseries.SerdeUtils;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.aggregation.postprocessors.TimeSeriesFn;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
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

public class MeanTimeSeriesAggregatorFactory extends BaseTimeSeriesAggregatorFactory
{
  private MeanTimeSeriesAggregatorFactory(
      String name,
      @Nullable String dataColumn,
      @Nullable String timeColumn,
      @Nullable String timeseriesColumn,
      @Nullable List<TimeSeriesFn> postProcessing,
      Long timeBucketMillis,
      Interval window,
      int maxEntries
  )
  {
    super(name, dataColumn, timeColumn, timeseriesColumn, postProcessing, timeBucketMillis, window, maxEntries);
  }

  @JsonCreator
  public static MeanTimeSeriesAggregatorFactory getMeanTimeSeriesAggregationFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("dataColumn") @Nullable final String dataColumn,
      @JsonProperty("timeColumn") @Nullable final String timeColumn,
      @JsonProperty("timeseriesColumn") @Nullable final String timeseriesColumn,
      @JsonProperty("postProcessing") @Nullable final List<TimeSeriesFn> postProcessing,
      @JsonProperty("timeBucketMillis") final Long timeBucketMillis,
      @JsonProperty("window") Interval window,
      @JsonProperty("maxEntries") @Nullable final Integer maxEntries
  )
  {
    boolean isPrebuilt = timeseriesColumn != null;
    boolean isRawData = dataColumn != null && timeColumn != null;
    if (isPrebuilt == isRawData) {
      throw new IAE("Must exclusively have a valid, non-null (timeColumn, dataColumn) or timeseriesColumn");
    }
    if (timeBucketMillis == null) {
      throw new IAE("Must have a valid, non-null timeBucketMillis");
    }
    if (window == null) {
      throw new IAE("Must exclusively have a valid, non-null window");
    }
    int finalMaxEntries = (int) Math.ceil(window.toDurationMillis() * 1D / timeBucketMillis);
    if (window.getStartMillis() % timeBucketMillis != 0) {
      finalMaxEntries++;
    }
    if (maxEntries != null && maxEntries > finalMaxEntries) {
      finalMaxEntries = maxEntries;
    }
    return new MeanTimeSeriesAggregatorFactory(name,
                                               dataColumn,
                                               timeColumn,
                                               timeseriesColumn,
                                               postProcessing,
                                               timeBucketMillis,
                                               window,
                                               finalMaxEntries);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      throw new UnsupportedOperationException();
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new MeanTimeSeriesAggregator(timeSelector,
                                          dataSelector,
                                          new DurationGranularity(getTimeBucketMillis(), 0),
                                          getwindow(),
                                          getMaxEntries());
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      BaseObjectColumnValueSelector<MeanTimeSeries> selector = metricFactory.makeColumnValueSelector(getTimeseriesColumn());
      return new MeanTimeSeriesMergeBufferAggregator(selector,
                                                     new DurationGranularity(getTimeBucketMillis(), 0),
                                                     getwindow(),
                                                     getMaxEntries());
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new MeanTimeSeriesBuildBufferAggregator(timeSelector,
                                                     dataSelector,
                                                     new DurationGranularity(getTimeBucketMillis(), 0),
                                                     getwindow(),
                                                     getMaxEntries());
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.AVG_TIMESERIES_CACHE_ID);
    super.addCacheKeys(builder);
    return builder.build();
  }

  @Nullable
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    MeanTimeSeries leftSeries = (MeanTimeSeries) lhs;
    MeanTimeSeries rightSeries = (MeanTimeSeries) rhs;
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
    return new MeanTimeSeriesAggregatorFactory(getName(),
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
    if (object instanceof MeanTimeSeries) {
      return object;
    }
    Map<String, Object> timeseriesKeys = (Map<String, Object>) object;
    ImplyLongArrayList bucketStarts = new ImplyLongArrayList((List<Long>) timeseriesKeys.get("bucketStarts"));
    ImplyDoubleArrayList sumPoints = new ImplyDoubleArrayList((List<Double>) timeseriesKeys.get("sumPoints"));
    ImplyLongArrayList countPoints = new ImplyLongArrayList((List<Number>) timeseriesKeys.get("countPoints"));
    NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> bounds = SerdeUtils.getBounds(timeseriesKeys);
    return new MeanTimeSeries(bucketStarts,
                              sumPoints,
                              countPoints,
                              new DurationGranularity(getTimeBucketMillis(), 0),
                              getwindow(),
                              bounds.lhs,
                              bounds.rhs,
                              getMaxEntries());
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object == null) {
      return null;
    }

    MeanTimeSeries meanTimeSeries = (MeanTimeSeries) object;
    meanTimeSeries.build();
    return super.finalizeComputation(meanTimeSeries.computeSimple());
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.ofComplex("imply-ts-avg");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return maxEntries * (Long.BYTES + Double.BYTES) + ByteBufferTimeSeries.DATA_OFFSET;
  }
}
