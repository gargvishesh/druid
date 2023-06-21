/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation.postprocessors;

import com.google.common.base.Preconditions;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.interpolation.Interpolator;
import org.apache.druid.java.util.common.granularity.DurationGranularity;

import javax.annotation.Nullable;

public class InterpolatorTimeSeriesFn implements TimeSeriesFn
{
  private final Long timeBucketMillis;
  private final Interpolator interpolator;
  private final boolean keepBoundariesOnly;

  public InterpolatorTimeSeriesFn(
      @Nullable final Long timeBucketMillis,
      @Nullable final Interpolator interpolator,
      boolean keepBoundariesOnly
  )
  {
    this.timeBucketMillis = Preconditions.checkNotNull(timeBucketMillis);
    this.interpolator = Preconditions.checkNotNull(interpolator);
    this.keepBoundariesOnly = keepBoundariesOnly;
  }

  @Override
  public SimpleTimeSeriesContainer compute(SimpleTimeSeries input, int maxEntries)
  {
    return interpolator.interpolate(
        input,
        new DurationGranularity(timeBucketMillis, 0),
        maxEntries,
        keepBoundariesOnly
    );
  }

}
