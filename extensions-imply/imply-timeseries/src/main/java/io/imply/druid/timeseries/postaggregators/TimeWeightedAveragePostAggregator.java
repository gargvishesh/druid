/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.postaggregators;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.aggregation.postprocessors.TimeWeightedAvgTimeSeriesFn;
import io.imply.druid.timeseries.interpolation.Interpolator;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Map;

@EverythingIsNonnullByDefault
public class TimeWeightedAveragePostAggregator extends InterpolationPostAggregator
{
  @JsonCreator
  public TimeWeightedAveragePostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("interpolator") final Interpolator interpolator,
      @JsonProperty("timeBucketMillis") final Long timeBucketMillis)
  {
    super(name, field, interpolator, timeBucketMillis);
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.TWA_POST_AGG_CACHE_ID)
        .appendCacheable(getField())
        .appendString(getInterpolator().name())
        .appendLong(getTimeBucketMillis());
    return builder.build();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    TimeSeries<?> timeSeries = ((TimeSeries<?>) getField().compute(combinedAggregators));
    if (timeSeries == null) {
      return null;
    }

    timeSeries.build();
    SimpleTimeSeries simpleTimeSeries = timeSeries.computeSimple();

    TimeWeightedAvgTimeSeriesFn timeWeightedAvgTimeSeriesFn = new TimeWeightedAvgTimeSeriesFn(getTimeBucketMillis(), getInterpolator());
    return timeWeightedAvgTimeSeriesFn.compute(simpleTimeSeries, simpleTimeSeries.getMaxEntries());
  }

  @Nullable
  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.ofComplex("imply-ts-simple");
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }
}
