/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.error.DruidException;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * This time series maintains a simple list of (time, data) tuples.
 */
public class SimpleTimeSeries extends TimeSeries<SimpleTimeSeries>
{
  private ImplyLongArrayList timestamps;
  private ImplyDoubleArrayList dataPoints;
  private final int maxEntries;
  private Long bucketMillis;
  private boolean isDirty;

  public SimpleTimeSeries(Interval window, int maxEntries)
  {
    this(new ImplyLongArrayList(), new ImplyDoubleArrayList(), window, null, null, maxEntries, 1L);
  }

  public SimpleTimeSeries(ImplyLongArrayList timestamps, ImplyDoubleArrayList dataPoints, Interval window, int maxEntries)
  {
    this(timestamps, dataPoints, window, null, null, maxEntries, 1L);
  }

  public SimpleTimeSeries(
      ImplyLongArrayList timestamps,
      ImplyDoubleArrayList dataPoints,
      Interval window,
      @Nullable EdgePoint start,
      @Nullable EdgePoint end,
      int maxEntries,
      Long bucketMillis
  )
  {
    super(window, start, end, maxEntries);
    this.timestamps = timestamps;
    this.dataPoints = dataPoints;
    this.maxEntries = maxEntries;
    this.bucketMillis = bucketMillis;
  }

  public SimpleTimeSeries copyWithWindow(Interval newWindow)
  {
    return copyWithWindowAndMaxEntries(newWindow, maxEntries);
  }

  public SimpleTimeSeries copyWithWindowAndMaxEntries(Interval newWindow, int newMaxEntries)
  {
    SimpleTimeSeries newSimpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(),
        new ImplyDoubleArrayList(),
        newWindow,
        null,
        null,
        newMaxEntries,
        getBucketMillis()
    );

    for (int i = 0; i < timestamps.size(); i++) {
      long timestamp = timestamps.getLong(i);
      double datapoint = dataPoints.getDouble(i);

      newSimpleTimeSeries.addDataPoint(timestamp, datapoint);
    }
    if (getStart().getTimestamp() != -1) {
      newSimpleTimeSeries.addDataPoint(getStart().getTimestamp(), getStart().getData());
    }
    if (getEnd().getTimestamp() != -1) {
      newSimpleTimeSeries.addDataPoint(getEnd().getTimestamp(), getEnd().getData());
    }

    return newSimpleTimeSeries;
  }

  public SimpleTimeSeries copyWithMaxEntries(int newMaxEntries)
  {
    if (size() > newMaxEntries) {
      throw new RuntimeException("Exceeded the max entries allowed");
    }
    return new SimpleTimeSeries(
        getTimestamps(),
        getDataPoints(),
        getWindow(),
        getStart(),
        getEnd(),
        newMaxEntries,
        getBucketMillis()
    );
  }

  @JsonProperty
  public ImplyLongArrayList getTimestamps()
  {
    if (isDirty) {
      computeSimple();
    }
    return timestamps;
  }

  @JsonProperty
  public ImplyDoubleArrayList getDataPoints()
  {
    if (isDirty) {
      computeSimple();
    }
    return dataPoints;
  }

  @JsonProperty
  public Long getBucketMillis()
  {
    return bucketMillis;
  }

  public void setBucketMillis(Long bucketMillis)
  {
    this.bucketMillis = bucketMillis;
  }

  @Override
  public int size()
  {
    return timestamps.size();
  }

  @Override
  protected void internalAddDataPoint(long timestamp, double data)
  {
    if (size() == maxEntries) {
      throw new RuntimeException("Exceeded the max entries allowed");
    }
    timestamps.add(timestamp);
    dataPoints.add(data);
    isDirty = true;
  }

  @Override
  public void addTimeSeries(SimpleTimeSeries timeSeries)
  {
    if (timeSeries == null) {
      return;
    }

    if (!getWindow().equals(timeSeries.getWindow())) {
      throw DruidException.defensive(
          "The time series to merge have different visible windows : (%s, %s)",
          getWindow(),
          timeSeries.getWindow()
      );
    }

    if (bucketMillis != null && !bucketMillis.equals(timeSeries.getBucketMillis())) {
      bucketMillis = null;
    }

    ImplyLongArrayList timestamps = timeSeries.getTimestamps();
    ImplyDoubleArrayList datapoints = timeSeries.getDataPoints();

    for (int i = 0; i < timestamps.size(); i++) {
      addDataPoint(timestamps.getLong(i), datapoints.getDouble(i));
    }

    final EdgePoint startBoundary = timeSeries.getStart();
    if (startBoundary != null && startBoundary.getTimestampJson() != null) {
      addDataPoint(startBoundary.getTimestamp(), startBoundary.getData());
    }

    final EdgePoint endBoundary = timeSeries.getEnd();
    if (endBoundary != null && endBoundary.getTimestampJson() != null) {
      addDataPoint(endBoundary.getTimestamp(), endBoundary.getData());
    }
    isDirty = true;
  }

  @Override
  public SimpleTimeSeries computeSimple()
  {
    if (isDirty) {
      cosort(timestamps, dataPoints);
      isDirty = false;
    }
    return this;
  }

  private static void cosort(ImplyLongArrayList timestamps, ImplyDoubleArrayList dataPoints)
  {
    List<TimestampDataPointPair> sortedPairList = new ArrayList<>();

    for (int i = 0; i < timestamps.size(); i++) {
      sortedPairList.add(new TimestampDataPointPair(timestamps.getLong(i), dataPoints.getDouble(i)));
    }

    sortedPairList.sort(Comparator.comparingLong(a -> a.timestamp));

    timestamps.clear();
    dataPoints.clear();

    sortedPairList.forEach(p -> {
      timestamps.add(p.timestamp);
      dataPoints.add(p.dataPoint);
    });
  }

  private static class TimestampDataPointPair
  {
    private final long timestamp;
    private final double dataPoint;

    public TimestampDataPointPair(long timestamp, double dataPoint)
    {
      this.timestamp = timestamp;
      this.dataPoint = dataPoint;
    }
  }

  @Override
  protected void internalCopy(SimpleTimeSeries copySeries)
  {
    this.timestamps = copySeries.getTimestamps();
    this.dataPoints = copySeries.getDataPoints();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamps, dataPoints, bucketMillis, super.hashCode());
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
    SimpleTimeSeries that = (SimpleTimeSeries) o;
    return Objects.equals(getTimestamps(), that.getTimestamps()) &&
           Objects.equals(getDataPoints(), that.getDataPoints()) &&
           Objects.equals(getBucketMillis(), that.getBucketMillis()) &&
           super.equals(o);
  }

  @Override
  public String toString()
  {
    return "SimpleTimeSeries{" +
           "timestamps=" + timestamps +
           ", dataPoints=" + dataPoints +
           ", maxEntries=" + maxEntries +
           ", start=" + getStart() +
           ", end=" + getEnd() +
           ", bucketMillis=" + getBucketMillis() +
           '}';
  }

  public SimpleTimeSeriesData asSimpleTimeSeriesData()
  {
    if (isDirty) {
      computeSimple();
    }
    return new SimpleTimeSeriesData(timestamps, dataPoints, getWindow());
  }
}
