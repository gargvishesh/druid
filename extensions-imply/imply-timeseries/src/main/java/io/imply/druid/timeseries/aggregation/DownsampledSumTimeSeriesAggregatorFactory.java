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
import io.imply.druid.timeseries.DownsampledSumTimeSeries;
import io.imply.druid.timeseries.SerdeUtils;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.TimeSeriesFromByteBufferAdapter;
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

public class DownsampledSumTimeSeriesAggregatorFactory extends BaseTimeSeriesAggregatorFactory
{
  private DownsampledSumTimeSeriesAggregatorFactory(
      String name,
      @Nullable String dataColumn,
      @Nullable String timeColumn,
      @Nullable String timeseriesColumn,
      Long timeBucketMillis,
      Interval window,
      int maxEntries
  )
  {
    super(name, dataColumn, timeColumn, timeseriesColumn, timeBucketMillis, window, maxEntries);
  }

  @JsonCreator
  public static DownsampledSumTimeSeriesAggregatorFactory getDownsampledSumTimeSeriesAggregationFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("dataColumn") @Nullable final String dataColumn,
      @JsonProperty("timeColumn") @Nullable final String timeColumn,
      @JsonProperty("timeseriesColumn") @Nullable final String timeseriesColumn,
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
    int finalMaxEntries = Math.toIntExact((long) Math.ceil(window.toDurationMillis() * 1D / timeBucketMillis));
    if (window.getStartMillis() % timeBucketMillis != 0) {
      finalMaxEntries++;
    }
    if (maxEntries != null && maxEntries > finalMaxEntries) {
      finalMaxEntries = maxEntries;
    }
    return new DownsampledSumTimeSeriesAggregatorFactory(
        name,
        dataColumn,
        timeColumn,
        timeseriesColumn,
        timeBucketMillis,
        window,
        finalMaxEntries
    );
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      throw new UnsupportedOperationException();
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new DownsampledSumTimeSeriesAggregator(
          timeSelector,
          dataSelector,
          new DurationGranularity(getTimeBucketMillis(), 0),
          getWindow(),
          getMaxEntries()
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (getTimeseriesColumn() != null) {
      BaseObjectColumnValueSelector<DownsampledSumTimeSeries> selector = metricFactory.makeColumnValueSelector(getTimeseriesColumn());
      return new DownsampledSumTimeSeriesMergeBufferAggregator(
          selector,
          new DurationGranularity(getTimeBucketMillis(), 0),
          getWindow(),
          getMaxEntries()
      );
    } else {
      BaseDoubleColumnValueSelector dataSelector = metricFactory.makeColumnValueSelector(getDataColumn());
      BaseLongColumnValueSelector timeSelector = metricFactory.makeColumnValueSelector(getTimeColumn());
      return new DownsampledSumTimeSeriesBuildBufferAggregator(
          timeSelector,
          dataSelector,
          new DurationGranularity(getTimeBucketMillis(), 0),
          getWindow(),
          getMaxEntries()
      );
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.DOWNSAMPLED_SUM_TIMESERIES_CACHE_ID);
    super.addCacheKeys(builder);
    return builder.build();
  }

  @Nullable
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    DownsampledSumTimeSeries leftSeries = (DownsampledSumTimeSeries) lhs;
    DownsampledSumTimeSeries rightSeries = (DownsampledSumTimeSeries) rhs;
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
    return new DownsampledSumTimeSeriesAggregatorFactory(
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
    if (object instanceof DownsampledSumTimeSeries) {
      return object;
    }
    Map<String, Object> timeseriesKeys = (Map<String, Object>) object;
    ImplyLongArrayList bucketStarts = new ImplyLongArrayList((List<Long>) timeseriesKeys.get("bucketStarts"));
    ImplyDoubleArrayList sumPoints = new ImplyDoubleArrayList((List<Double>) timeseriesKeys.get("sumPoints"));
    NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> bounds = SerdeUtils.getBounds(timeseriesKeys);
    return new DownsampledSumTimeSeries(
        bucketStarts,
        sumPoints,
        new DurationGranularity(getTimeBucketMillis(), 0),
        getWindow(),
        bounds.lhs,
        bounds.rhs,
        getMaxEntries()
    );
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object == null) {
      return null;
    }

    DownsampledSumTimeSeries downsampledSumTimeSeries = (DownsampledSumTimeSeries) object;
    downsampledSumTimeSeries.build();
    return SimpleTimeSeriesContainer.createFromInstance(downsampledSumTimeSeries.computeSimple());
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.ofComplex("imply-ts-downsampled-sum");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return (int) (maxEntries * Double.BYTES + Math.ceil((double) maxEntries / Byte.SIZE) + TimeSeriesFromByteBufferAdapter.DATA_OFFSET);
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DownsampledSumTimeSeriesAggregatorFactory(
        newName,
        getDataColumn(),
        getTimeColumn(),
        getTimeseriesColumn(),
        getTimeBucketMillis(),
        getWindow(),
        getMaxEntries()
    );
  }

  @Override
  public String toString()
  {
    return "DownsampledSumTimeSeriesAggregatorFactory{" +
           "name='" + name + '\'' +
           ", dataColumn='" + dataColumn + '\'' +
           ", timeColumn='" + timeColumn + '\'' +
           ", timeseriesColumn='" + timeseriesColumn + '\'' +
           ", timeBucketMillis=" + timeBucketMillis +
           ", window=" + window +
           ", maxEntries=" + maxEntries +
           '}';
  }
}
