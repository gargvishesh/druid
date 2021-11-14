/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation.postprocessors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.interpolation.Interpolator;
import org.apache.druid.java.util.common.granularity.DurationGranularity;

import javax.annotation.Nullable;

public class InterpolatorTimeSeriesFn implements TimeSeriesFn
{
  private final Long timeBucketMillis;
  private final Interpolator interpolator;

  @JsonCreator
  public InterpolatorTimeSeriesFn(
      @JsonProperty("timeBucketMillis") @Nullable final Long timeBucketMillis,
      @JsonProperty("interpolator") @Nullable final Interpolator interpolator
  )
  {
    this.timeBucketMillis = Preconditions.checkNotNull(timeBucketMillis);
    this.interpolator = Preconditions.checkNotNull(interpolator);
  }

  @JsonProperty
  public Long getTimeBucketMillis()
  {
    return timeBucketMillis;
  }

  @JsonProperty
  public Interpolator getInterpolator()
  {
    return interpolator;
  }

  @Override
  public SimpleTimeSeries compute(SimpleTimeSeries input, int maxEntries)
  {
    return interpolator.interpolate(input,
                                    new DurationGranularity(timeBucketMillis, 0),
                                    maxEntries);
  }

  @Override
  public String cacheString()
  {
    return String.join(",", "interpolation", String.valueOf(timeBucketMillis), interpolator.toString());
  }
}
