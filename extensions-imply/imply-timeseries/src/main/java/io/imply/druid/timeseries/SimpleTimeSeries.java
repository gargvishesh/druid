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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.collect.Utils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This time series maintains a simple list of (time, data) tuples.
 */
public class SimpleTimeSeries extends TimeSeries<SimpleTimeSeries>
{
  private ImplyLongArrayList timestamps;
  private ImplyDoubleArrayList dataPoints;
  private final int maxEntries;
  private Long bucketMillis;

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

  public SimpleTimeSeries withWindow(Interval newWindow)
  {
    SimpleTimeSeries newSimpleTimeSeries = new SimpleTimeSeries(newWindow, maxEntries);

    for (int i = 0; i < timestamps.size(); i++) {
      long timestamp = timestamps.getLong(i);
      double datapoint = dataPoints.getDouble(i);

      newSimpleTimeSeries.addDataPoint(timestamp, datapoint);
    }

    return newSimpleTimeSeries;
  }

  @JsonProperty
  public ImplyLongArrayList getTimestamps()
  {
    return timestamps;
  }

  @JsonProperty
  public ImplyDoubleArrayList getDataPoints()
  {
    return dataPoints;
  }

  @JsonProperty
  public Long getBucketMillis()
  {
    return bucketMillis;
  }

  public List<SimpleTimeSeries> getTimeSeriesList()
  {
    return timeSeriesList;
  }

  @Override
  public int size()
  {
    build();
    return timestamps.size() + timeSeriesList.stream().mapToInt(SimpleTimeSeries::size).sum();
  }

  @Override
  protected void internalAddDataPoint(long timestamp, double data)
  {
    if (size() == maxEntries) {
      throw new RuntimeException("Exceeded the max entries allowed");
    }
    timestamps.add(timestamp);
    dataPoints.add(data);
  }

  @Override
  protected void internalMergeSeries(List<SimpleTimeSeries> mergeSeries)
  {
    if (mergeSeries.isEmpty()) {
      return;
    }

    mergeSeries.forEach(SimpleTimeSeries::build);
    Iterator<Pair<Long, Double>> mergedTimeSeries = Utils.mergeSorted(
        mergeSeries.stream()
                   .map(SimpleTimeSeries::getIterator)
                   .collect(Collectors.toList()),
        Comparator.comparingLong(lhs -> lhs.lhs)
    );

    ImplyLongArrayList timestamps = new ImplyLongArrayList(size());
    ImplyDoubleArrayList dataPoints = new ImplyDoubleArrayList(size());
    while (mergedTimeSeries.hasNext()) {
      if (size() == maxEntries) {
        throw new RuntimeException("Exceeded the max entries allowed");
      }
      Pair<Long, Double> dataPoint = mergedTimeSeries.next();
      timestamps.add((long) dataPoint.lhs);
      dataPoints.add((double) dataPoint.rhs);
    }
    SimpleTimeSeries mergedSeries = new SimpleTimeSeries(
        timestamps,
        dataPoints,
        getWindow(),
        getStart(),
        getEnd(),
        getMaxEntries(),
        1L
    );
    mergedSeries.build();
    copy(mergedSeries);
  }

  @Override
  public void addTimeSeries(SimpleTimeSeries timeSeries)
  {
    for (int i = 1; i < timeSeries.size(); i++) {
      if (timeSeries.getTimestamps().getLong(i - 1) > timeSeries.getTimestamps().getLong(i)) {
        throw new RE(
            "SimpleTimeSeries data is not sorted." + "Found timestamp %d after %d while merging",
            timeSeries.getTimestamps().getLong(i),
            timeSeries.getTimestamps().getLong(i - 1)
        );
      }
    }
    if (bucketMillis != null && !bucketMillis.equals(timeSeries.getBucketMillis())) {
      bucketMillis = null;
    }
    timeSeriesList.add(timeSeries);
  }

  @Override
  public void build()
  {
    if (timeSeriesList.isEmpty()) {
      return;
    }
    List<SimpleTimeSeries> mergeList = new ArrayList<>(timeSeriesList);
    timeSeriesList.clear();
    mergeList.add(this);
    mergeSeries(mergeList);
  }

  @Override
  public SimpleTimeSeries computeSimple()
  {
    build();
    cosort(timestamps, dataPoints);
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
    this.timeSeriesList = copySeries.getTimeSeriesList();
  }

  private Iterator<Pair<Long, Double>> getIterator()
  {
    return IntStream.range(0, timestamps.size())
                    .mapToObj(idx -> new Pair<>(timestamps.getLong(idx), dataPoints.getDouble(idx)))
                    .iterator();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamps, dataPoints, timeSeriesList, bucketMillis, super.hashCode());
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
    return Objects.equals(timestamps, that.timestamps) && Objects.equals(dataPoints, that.dataPoints) && Objects.equals(
        timeSeriesList,
        that.timeSeriesList
    ) && getBucketMillis().equals(that.getBucketMillis()) && super.equals(o);
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
    return new SimpleTimeSeriesData(timestamps, dataPoints, getWindow());
  }
}
