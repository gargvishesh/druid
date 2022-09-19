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
import com.google.common.base.Preconditions;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.interpolation.Interpolator;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@EverythingIsNonnullByDefault
public class InterpolationPostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;
  private final Interpolator interpolator;
  private final long timeBucketMillis;

  @JsonCreator
  public InterpolationPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("interpolator") final Interpolator interpolator,
      @JsonProperty("timeBucketMillis") final Long timeBucketMillis)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.interpolator = Preconditions.checkNotNull(interpolator, "interpolator is null");
    this.timeBucketMillis = Preconditions.checkNotNull(timeBucketMillis, "timeBucketMillis is null");
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public long getTimeBucketMillis()
  {
    return timeBucketMillis;
  }

  @JsonProperty
  public Interpolator getInterpolator()
  {
    return interpolator;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.INTERPOLATION_POST_AGG_CACHE_ID)
        .appendCacheable(getField())
        .appendString(getInterpolator().name())
        .appendLong(getTimeBucketMillis());
    return builder.build();
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public Comparator getComparator()
  {
    throw new IAE("Comparing timeseries is not supported");
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    Object computedField = field.compute(combinedAggregators);
    TimeSeries<?> timeSeries = PostAggregatorsUtil.asTimeSeries(computedField);

    if (timeSeries == null) {
      return null;
    }

    timeSeries.computeSimple();
    SimpleTimeSeries simpleTimeSeries = timeSeries.computeSimple();

    return interpolator.interpolate(simpleTimeSeries, new DurationGranularity(timeBucketMillis, 0), simpleTimeSeries.getMaxEntries());
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
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

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", interpolator=" + interpolator +
           ", timeBucketMillis=" + timeBucketMillis +
           "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InterpolationPostAggregator that = (InterpolationPostAggregator) o;
    return Objects.equals(name, that.getName()) &&
           Objects.equals(field, that.getField()) &&
           Objects.equals(interpolator, that.getInterpolator()) &&
           Objects.equals(timeBucketMillis, that.getTimeBucketMillis());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, interpolator, timeBucketMillis);
  }
}
