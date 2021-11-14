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
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.granularity.DurationGranularity;

import javax.annotation.Nullable;

public class TimeWeightedAvgTimeSeriesFn implements TimeSeriesFn
{
  private final Long timeBucketMillis;
  private final Interpolator interpolator;

  @JsonCreator
  public TimeWeightedAvgTimeSeriesFn(
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
    if (input.size() <= 1) {
      return input;
    }

    ImplyLongArrayList timestamps = input.getTimestamps();
    ImplyDoubleArrayList dataPoints = input.getDataPoints();
    DurationGranularity durationGranularity = new DurationGranularity(timeBucketMillis, 0);
    SimpleTimeSeries computedSeries = new SimpleTimeSeries(new ImplyLongArrayList(maxEntries),
                                                           new ImplyDoubleArrayList(maxEntries),
                                                           input.getwindow(),
                                                           input.getStart(),
                                                           input.getEnd(),
                                                           maxEntries);
    // compute initial recordings
    long currentTimestamp = timestamps.getLong(0);
    long currentBucketStart = durationGranularity.bucketStart(currentTimestamp);
    double currentDataPoint = dataPoints.getDouble(0);
    double currentBucketTimeWeightedSum = 0;
    long currentBucketTimeWeightedCount = 0;
    if (currentTimestamp != currentBucketStart) {
      Double val = interpolator.interpolateStart(input, currentBucketStart);
      if (val != null) {
        currentBucketTimeWeightedSum += interpolator.computeIntegral(currentBucketStart, val, currentTimestamp, currentDataPoint);
        currentBucketTimeWeightedCount += currentTimestamp - currentBucketStart;
      }
    }
    long prevTimestamp = currentTimestamp;
    long prevBucketStart = currentBucketStart;
    double prevDataPoint = currentDataPoint;
    for (int i = 1; i < input.size(); i++) {
      currentTimestamp = timestamps.getLong(i);
      currentBucketStart = durationGranularity.bucketStart(currentTimestamp);
      currentDataPoint = dataPoints.getDouble(i);

      if (currentTimestamp < prevTimestamp) {
        throw new RE("Unsorted data points while calculating time weighted average. Found %d timestamp after %d", currentTimestamp, prevTimestamp);
      }
      if (currentTimestamp == prevTimestamp) {
        // ignoring the second values in this case. not ideal, but is ok (followed the idea on https://github.com/timescale/timescaledb-toolkit/discussions/65)
        continue;
      }

      if (prevBucketStart == currentBucketStart) {
        currentBucketTimeWeightedSum += interpolator.computeIntegral(prevTimestamp, prevDataPoint, currentTimestamp, currentDataPoint);
        currentBucketTimeWeightedCount += currentTimestamp - prevTimestamp;
      } else {
        // add average for previous bucket
        if (prevTimestamp != prevBucketStart + durationGranularity.getDurationMillis()) { // last wasn't end of the bucket
          long prevBucketEnd = prevBucketStart + durationGranularity.getDurationMillis();
          double prevBucketEndDataPoint = interpolator.interpolate(prevTimestamp, prevDataPoint, currentTimestamp, currentDataPoint, prevBucketEnd);
          currentBucketTimeWeightedSum += interpolator.computeIntegral(prevTimestamp, prevDataPoint, prevBucketEnd, prevBucketEndDataPoint);
          currentBucketTimeWeightedCount += prevBucketEnd - prevTimestamp;
        }
        computedSeries.addDataPoint(prevBucketStart, currentBucketTimeWeightedCount > 0 ? currentBucketTimeWeightedSum / currentBucketTimeWeightedCount : 0);

        if (currentTimestamp != currentBucketStart) {
          double currentBucketStartDataPoint = interpolator.interpolate(prevTimestamp, prevDataPoint, currentTimestamp, currentDataPoint, currentBucketStart);
          currentBucketTimeWeightedSum += interpolator.computeIntegral(currentBucketStart, currentBucketStartDataPoint, currentTimestamp, currentDataPoint);
          currentBucketTimeWeightedCount += currentTimestamp - currentBucketStart;
        } else {
          currentBucketTimeWeightedSum = 0;
          currentBucketTimeWeightedCount = 0;
        }
      }
      prevTimestamp = currentTimestamp;
      prevDataPoint = currentDataPoint;
      prevBucketStart = currentBucketStart;
    }
    // add average for last bucket
    long prevBucketEnd = prevBucketStart + durationGranularity.getDurationMillis();
    if (prevTimestamp != prevBucketEnd) { // last wasn't end of the bucket
      Double val = interpolator.interpolateEnd(input, prevBucketEnd);
      if (val != null) {
        currentBucketTimeWeightedSum += interpolator.computeIntegral(prevTimestamp, prevDataPoint, prevBucketEnd, val);
        currentBucketTimeWeightedCount += prevBucketEnd - prevTimestamp;
      }
    }
    computedSeries.addDataPoint(prevBucketStart, currentBucketTimeWeightedCount > 0 ? currentBucketTimeWeightedSum / currentBucketTimeWeightedCount : 0);
    return computedSeries;
  }

  @Override
  public String cacheString()
  {
    return String.join(",", "timeWeightedAverage", String.valueOf(timeBucketMillis), interpolator.toString());
  }
}
