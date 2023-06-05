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
import com.google.common.base.Preconditions;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.aggregation.postprocessors.AggregateOperators;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class SumTimeSeriesAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final String timeseriesColumn;
  private final int maxEntries;
  private final Interval window = Intervals.ETERNITY;
  public static final ColumnType TYPE = ColumnType.ofComplex("imply-ts-simple");

  private SumTimeSeriesAggregatorFactory(String name, String timeseriesColumn, int maxEntries)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.timeseriesColumn = Preconditions.checkNotNull(timeseriesColumn, "time series column is null");
    this.maxEntries = maxEntries;
  }

  @JsonCreator
  public static SumTimeSeriesAggregatorFactory getTimeSeriesAggregationFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("timeseriesColumn") final String timeseriesColumn,
      @JsonProperty("maxEntries") @Nullable final Integer maxEntries
  )
  {
    return new SumTimeSeriesAggregatorFactory(
        name,
        timeseriesColumn,
        GuavaUtils.firstNonNull(maxEntries, SimpleTimeSeriesAggregatorFactory.DEFAULT_MAX_ENTRIES)
    );
  }

  @JsonProperty
  public String getTimeseriesColumn()
  {
    return timeseriesColumn;
  }
  @JsonProperty
  public int getMaxEntries()
  {
    return maxEntries;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.SUM_TIMESERIES_CACHE_ID);
    builder.appendString(getName())
           .appendString(getTimeseriesColumn())
           .appendInt(getMaxEntries());
    return builder.build();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new SumTimeSeriesAggregator(
        metricFactory.makeColumnValueSelector(getTimeseriesColumn()),
        window,
        getMaxEntries()
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new SumTimeSeriesBufferAggregator(
        metricFactory.makeColumnValueSelector(getTimeseriesColumn()),
        window,
        getMaxEntries()
    );
  }

  @Override
  public Comparator getComparator()
  {
    throw new UOE("%s aggregation doesn't support comparisons", TimeSeriesModule.SUM_TIMESERIES);
  }

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

    SimpleTimeSeries leftSimple = leftSeries.getSimpleTimeSeries().computeSimple();
    SimpleTimeSeries rightSimple = rightSeries.getSimpleTimeSeries().computeSimple();
    AggregateOperators.addIdenticalTimestamps(
        leftSimple.getTimestamps().getLongArray(),
        leftSimple.getDataPoints().getDoubleArray(),
        rightSimple.getTimestamps().getLongArray(),
        rightSimple.getDataPoints().getDoubleArray(),
        leftSimple.size(),
        rightSimple.size()
    );
    return SimpleTimeSeriesContainer.createFromInstance(rightSimple);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SumTimeSeriesAggregatorFactory(getName(), getName(), getMaxEntries());
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UOE(
        "aggregation [%s] cannot getRequiredColumns(), "
        + "this is believed to only impact GroupByStrategyV1, which is unsupported",
        TimeSeriesModule.SUM_TIMESERIES
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    return SimpleTimeSeriesContainer.createFromObject(object, window, getMaxEntries());
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(getTimeseriesColumn());
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return (Long.BYTES + Double.BYTES) * getMaxEntries();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return TYPE;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new SumTimeSeriesAggregatorFactory(newName, getTimeseriesColumn(), getMaxEntries());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getName(), getTimeseriesColumn(), getMaxEntries());
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    SumTimeSeriesAggregatorFactory that = (SumTimeSeriesAggregatorFactory) obj;
    return Objects.equals(getName(), that.getName()) &&
           Objects.equals(getTimeseriesColumn(), that.getTimeseriesColumn()) &&
           Objects.equals(getMaxEntries(), that.getMaxEntries());
  }
}
