/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.interpolation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;

import javax.annotation.Nullable;

/**
 * This is used to fill the gaps in a sparse time series using one of the following strategies
 */
public enum Interpolator
{
  LINEAR {
    @Override
    public double interpolate(
        long previousTimestamp,
        double previousTimestampData,
        long nextTimestamp,
        double nextTimestampData,
        long interpolateTimestamp
    )
    {
      double slope = (nextTimestampData - previousTimestampData) / (nextTimestamp - previousTimestamp);
      return slope * (interpolateTimestamp - previousTimestamp) + previousTimestampData;
    }

    @Override
    public double computeIntegral(
        long previousTimestamp,
        double previousTimestampData,
        long nextTimestamp,
        double nextTimestampData
    )
    {
      return ((previousTimestampData + nextTimestampData) / 2) * (nextTimestamp - previousTimestamp);
    }
  },
  PADDING {
    @Override
    public double interpolate(
        long previousTimestamp,
        double previousTimestampData,
        long nextTimestamp,
        double nextTimestampData,
        long interpolateTimestamp
    )
    {
      return previousTimestamp < nextTimestamp ? previousTimestampData : nextTimestampData;
    }

    @Override
    public double computeIntegral(
        long previousTimestamp,
        double previousTimestampData,
        long nextTimestamp,
        double nextTimestampData
    )
    {
      return 0;
    }
  },
  BACKFILL {
    @Override
    public double interpolate(
        long previousTimestamp,
        double previousTimestampData,
        long nextTimestamp,
        double nextTimestampData,
        long interpolateTimestamp
    )
    {
      return previousTimestamp < nextTimestamp ? nextTimestampData : previousTimestampData;
    }

    @Override
    public double computeIntegral(
        long previousTimestamp,
        double previousTimestampData,
        long nextTimestamp,
        double nextTimestampData
    )
    {
      return 0;
    }
  };

  public abstract double interpolate(long previousTimestamp,
                              double previousTimestampData,
                              long nextTimestamp,
                              double nextTimestampData,
                              long interpolateTimestamp);

  public abstract double computeIntegral(long previousTimestamp,
                                  double previousTimestampData,
                                  long nextTimestamp,
                                  double nextTimestampData);

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toUpperCase(name());
  }

  @Nullable
  @JsonCreator
  public static Interpolator fromString(String name)
  {
    return name == null ? null : valueOf(StringUtils.toUpperCase(name));
  }

  public SimpleTimeSeries interpolate(SimpleTimeSeries inputSeries, DurationGranularity durationGranularity, int maxEntries)
  {
    if (inputSeries.size() == 0) {
      if (inputSeries.getStart() != null && inputSeries.getEnd() != null) {
        // interpolate visible window start and end if we have bounds
        SimpleTimeSeries timeSeries = new SimpleTimeSeries(new ImplyLongArrayList(),
                                                           new ImplyDoubleArrayList(),
                                                           inputSeries.getwindow(),
                                                           inputSeries.getStart(),
                                                           inputSeries.getEnd(),
                                                           maxEntries);
        timeSeries.addDataPoint(inputSeries.getwindow().getStartMillis(),
                                interpolate(inputSeries.getStart().getTimestamp(),
                                            inputSeries.getStart().getData(),
                                            inputSeries.getEnd().getTimestamp(),
                                            inputSeries.getEnd().getData(),
                                            inputSeries.getwindow().getStartMillis()));
        timeSeries.build();
        return timeSeries;
      }
      return inputSeries;
    }

    ImplyLongArrayList timestamps = inputSeries.getTimestamps();
    ImplyDoubleArrayList dataPoints = inputSeries.getDataPoints();
    SimpleTimeSeries timeSeries = new SimpleTimeSeries(new ImplyLongArrayList(maxEntries),
                                                       new ImplyDoubleArrayList(maxEntries),
                                                       inputSeries.getwindow(),
                                                       inputSeries.getStart(),
                                                       inputSeries.getEnd(),
                                                       maxEntries);

    long currTimestamp = timestamps.getLong(0);
    long currTimestampBucketStart = durationGranularity.bucketStart(timestamps.getLong(0));
    double currDataPoint = dataPoints.getDouble(0);
    // interpolate visible window start if we can
    Double windowStartDataPoint = interpolateStart(inputSeries, inputSeries.getwindow().getStartMillis());
    if (windowStartDataPoint != null) { // have a valid value for the start point
      timeSeries.addDataPoint(inputSeries.getwindow().getStartMillis(), windowStartDataPoint);
    }
    timeSeries.addDataPoint(currTimestamp, currDataPoint);

    long prevTimestamp = currTimestamp;
    long prevTimestampBucketStart = currTimestampBucketStart;
    double prevDataPoint = currDataPoint;
    for (int i = 1; i < timestamps.size(); i++) {
      currTimestamp = timestamps.getLong(i);
      currTimestampBucketStart = durationGranularity.bucketStart(currTimestamp);
      currDataPoint = dataPoints.getDouble(i);
      if (currTimestampBucketStart == prevTimestampBucketStart) {
        timeSeries.addDataPoint(currTimestamp, currDataPoint);
      } else if (currTimestampBucketStart > prevTimestampBucketStart) {
        int missingBucketsCount = (int) ((currTimestampBucketStart - prevTimestampBucketStart) / durationGranularity.getDurationMillis()) - 1;
        for (int j = 1; j <= missingBucketsCount; j++) {
          long missingTimestamp = prevTimestampBucketStart + j * durationGranularity.getDurationMillis();
          double missingDataPoint = interpolate(prevTimestamp,
                                                prevDataPoint,
                                                currTimestamp,
                                                currDataPoint,
                                                missingTimestamp);
          timeSeries.addDataPoint(missingTimestamp, missingDataPoint);
        }
        timeSeries.addDataPoint(currTimestamp, currDataPoint); // add the current data point
      } else {
        throw new IAE("Unordered time series");
      }
      prevTimestamp = currTimestamp;
      prevDataPoint = currDataPoint;
      prevTimestampBucketStart = currTimestampBucketStart;
    }

    Double windowEndDataPoint = interpolateEnd(inputSeries, inputSeries.getwindow().getEndMillis());
    long windowEndBucketStart = durationGranularity.bucketStart(inputSeries.getwindow().getEndMillis());
    if (windowEndDataPoint != null && windowEndBucketStart != prevTimestampBucketStart) {
      int missingBucketsCount = (int) ((windowEndBucketStart - prevTimestampBucketStart) / durationGranularity.getDurationMillis());
      if (inputSeries.getwindow().getEndMillis() == windowEndBucketStart) {
        missingBucketsCount--;
      }
      for (int j = 1; j <= missingBucketsCount; j++) {
        long missingTimestamp = prevTimestampBucketStart + j * durationGranularity.getDurationMillis();
        double missingDataPoint = interpolate(
            prevTimestamp,
            prevDataPoint,
            inputSeries.getwindow().getEndMillis(),
            windowEndDataPoint,
            missingTimestamp
        );
        timeSeries.addDataPoint(missingTimestamp, missingDataPoint);
      }
    }

    timeSeries.build();
    return timeSeries;
  }

  @Nullable
  public Double interpolateStart(SimpleTimeSeries inputSeries, long startTime)
  {
    if (inputSeries.size() == 0) {
      return null;
    }

    if (startTime > inputSeries.getTimestamps().getLong(0)) {
      throw new RuntimeException(("startTime should be before the first timestamp in the time series"));
    }

    if (inputSeries.getTimestamps().getLong(0) != startTime) {
      Double windowStartDataPoint = null;
      if (inputSeries.getStart().getTimestamp() != -1) { // do we have the start bound?
        windowStartDataPoint = interpolate(inputSeries.getStart().getTimestamp(),
                                                  inputSeries.getStart().getData(),
                                                  inputSeries.getTimestamps().getLong(0),
                                                  inputSeries.getDataPoints().getDouble(0),
                                                  startTime);
      } else if (inputSeries.size() >= 2) {
        windowStartDataPoint = interpolate(inputSeries.getTimestamps().getLong(0),
                                                  inputSeries.getDataPoints().getDouble(0),
                                                  inputSeries.getTimestamps().getLong(1),
                                                  inputSeries.getDataPoints().getDouble(1),
                                                  startTime);
      } else if (inputSeries.getEnd().getTimestamp() != -1) { // we have 1 point and the end bound
        windowStartDataPoint = interpolate(inputSeries.getTimestamps().getLong(0),
                                                  inputSeries.getDataPoints().getDouble(0),
                                                  inputSeries.getEnd().getTimestamp(),
                                                  inputSeries.getEnd().getData(),
                                                  startTime);
      }
      return windowStartDataPoint;
    }
    return null;
  }

  @Nullable
  public Double interpolateEnd(SimpleTimeSeries inputSeries, long endTime)
  {
    if (inputSeries.size() == 0) {
      return null;
    }

    if (endTime < inputSeries.getTimestamps().getLong(inputSeries.size() - 1)) {
      throw new RuntimeException(("endTime should be after the last timestamp in the time series"));
    }

    if (inputSeries.getTimestamps().getLong(inputSeries.size() - 1) != endTime) {
      Double windowEndDataPoint = null;
      if (inputSeries.getEnd().getTimestamp() != -1) { // do we have the end bound?
        windowEndDataPoint = interpolate(inputSeries.getTimestamps().getLong(inputSeries.size() - 1),
                                                inputSeries.getDataPoints().getDouble(inputSeries.size() - 1),
                                                inputSeries.getEnd().getTimestamp(),
                                                inputSeries.getEnd().getData(),
                                                endTime);
      } else if (inputSeries.size() >= 2) { // if the series has >= 2 elements, interpolate using them
        windowEndDataPoint = interpolate(inputSeries.getTimestamps().getLong(inputSeries.size() - 2),
                                                inputSeries.getDataPoints().getDouble(inputSeries.size() - 2),
                                                inputSeries.getTimestamps().getLong(inputSeries.size() - 1),
                                                inputSeries.getDataPoints().getDouble(inputSeries.size() - 1),
                                                endTime);
      } else if (inputSeries.getStart().getTimestamp() != -1) { // we have 1 point and the start bound
        windowEndDataPoint = interpolate(inputSeries.getStart().getTimestamp(),
                                                inputSeries.getStart().getData(),
                                                inputSeries.getTimestamps().getLong(inputSeries.size() - 1),
                                                inputSeries.getDataPoints().getDouble(inputSeries.size() - 1),
                                                endTime);
      }
      return windowEndDataPoint;
    }
    return null;
  }
}
