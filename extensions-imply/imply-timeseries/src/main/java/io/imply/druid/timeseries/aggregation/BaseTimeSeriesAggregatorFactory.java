/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.aggregation.postprocessors.TimeSeriesFn;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class BaseTimeSeriesAggregatorFactory extends AggregatorFactory
{
  protected final String name;
  @Nullable
  protected final String dataColumn;
  @Nullable
  protected final String timeColumn;
  @Nullable
  protected final String timeseriesColumn;
  @Nullable
  protected final List<TimeSeriesFn> postProcessing;
  @Nullable
  protected final Long timeBucketMillis;
  protected final Interval window;
  protected final int maxEntries;

  protected BaseTimeSeriesAggregatorFactory(
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
    this.name = name;
    this.dataColumn = dataColumn;
    this.timeColumn = timeColumn;
    this.timeseriesColumn = timeseriesColumn;
    this.postProcessing = postProcessing;
    this.timeBucketMillis = timeBucketMillis;
    this.window = window;
    this.maxEntries = maxEntries;
  }

  @Nonnull
  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @JsonProperty
  public String getDataColumn()
  {
    return dataColumn;
  }

  @Nullable
  @JsonProperty
  public String getTimeColumn()
  {
    return timeColumn;
  }

  @Nullable
  @JsonProperty
  public String getTimeseriesColumn()
  {
    return timeseriesColumn;
  }

  @Nullable
  @JsonProperty
  public Long getTimeBucketMillis()
  {
    return timeBucketMillis;
  }

  @Nullable
  @JsonProperty
  public List<TimeSeriesFn> getPostProcessing()
  {
    return postProcessing;
  }

  @Nonnull
  @JsonProperty
  public Interval getWindow()
  {
    return window;
  }


  @Nonnull
  @JsonProperty
  public int getMaxEntries()
  {
    return maxEntries;
  }

  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException();
  }

  protected void addCacheKeys(CacheKeyBuilder cacheKeyBuilder)
  {
    cacheKeyBuilder.appendString(getName())
                   .appendString(getTimeColumn())
                   .appendString(getDataColumn())
                   .appendString(getTimeseriesColumn())
                   .appendString(String.valueOf(getTimeBucketMillis()))
                   .appendString(String.valueOf(getWindow()))
                   .appendInt(getMaxEntries());
    if (getPostProcessing() != null) {
      cacheKeyBuilder.appendString(getPostProcessing().stream().map(TimeSeriesFn::cacheString).collect(Collectors.joining(",")));
    }
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object deserialize(Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object == null) {
      return null;
    }

    if (!(object instanceof SimpleTimeSeries)) {
      throw new RE("Found object of type %s in finalize", object.getClass());
    }

    SimpleTimeSeries finalResult = (SimpleTimeSeries) object;
    if (getPostProcessing() != null) {
      for (TimeSeriesFn postprocessor : getPostProcessing()) {
        finalResult = postprocessor.compute(finalResult, getMaxEntries());
      }
    }
    return finalResult;
  }

  @Override
  public List<String> requiredFields()
  {
    if (timeColumn == null || dataColumn == null) {
      return ImmutableList.of();
    }
    return ImmutableList.of(timeColumn, dataColumn);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.ofComplex("imply-ts-simple");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BaseTimeSeriesAggregatorFactory that = (BaseTimeSeriesAggregatorFactory) o;
    return maxEntries == that.maxEntries &&
           Objects.equals(name, that.name) &&
           Objects.equals(timeColumn, that.timeColumn) &&
           Objects.equals(dataColumn, that.dataColumn) &&
           Objects.equals(timeseriesColumn, that.timeseriesColumn) &&
           Objects.equals(postProcessing, that.postProcessing) &&
           Objects.equals(timeBucketMillis, that.timeBucketMillis) &&
           Objects.equals(window, that.window);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        name,
        dataColumn,
        timeColumn,
        timeseriesColumn,
        postProcessing,
        timeBucketMillis,
        window,
        maxEntries
    );
  }
}
