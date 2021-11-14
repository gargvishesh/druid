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
import com.google.common.collect.Iterators;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
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
 * This maintains an intermediate TS for tracking the delta change of all samples present per time bucket.
 * The time buckets are created on fly on the basis of the bucket duration as the data is read.
 */
public class DeltaTimeSeries extends TimeSeries<DeltaTimeSeries>
{
  private ImplyLongArrayList timestamps;
  private ImplyDoubleArrayList dataPoints;
  private DurationGranularity timeBucketGranularity;

  public DeltaTimeSeries(DurationGranularity timeBucketGranularity, Interval window, int maxEntries)
  {
    this(new ImplyLongArrayList(),
         new ImplyDoubleArrayList(),
         timeBucketGranularity,
         window,
         null,
         null,
         maxEntries);
  }

  public DeltaTimeSeries(ImplyLongArrayList timestamps,
                         ImplyDoubleArrayList dataPoints,
                         DurationGranularity timeBucketGranularity,
                         Interval window,
                         @Nullable EdgePoint start,
                         @Nullable EdgePoint end,
                         int maxEntries)
  {
    super(window, start, end, maxEntries);
    this.timestamps = timestamps;
    this.dataPoints = dataPoints;
    this.timeBucketGranularity = Objects.requireNonNull(timeBucketGranularity, "Must have a non-null duration");
  }

  private Iterator<BucketData> getIterator()
  {
    return IntStream.range(0, size())
                    .mapToObj(idx -> new BucketData(timestamps.getLong(2 * idx),
                                                    timestamps.getLong(2 * idx + 1),
                                                    dataPoints.getDouble(2 * idx),
                                                    dataPoints.getDouble(2 * idx + 1)))
                    .iterator();
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
  public DurationGranularity getTimeBucketGranularity()
  {
    return timeBucketGranularity;
  }

  public List<DeltaTimeSeries> getTimeSeriesList()
  {
    return timeSeriesList;
  }

  @Override
  public int size()
  {
    build();
    return timestamps.size() / 2 + timeSeriesList.stream().mapToInt(DeltaTimeSeries::size).sum();
  }

  @Override
  protected void internalAddDataPoint(long timestamp, double data)
  {
    long bucketStart = timeBucketGranularity.bucketStart(timestamp);
    int currMaxTsIndex = timestamps.size() - 1;
    long prevBucketStart = -1;
    if (currMaxTsIndex >= 0) {
      prevBucketStart = timeBucketGranularity.bucketStart(timestamps.getLong(currMaxTsIndex));
    }
    if (currMaxTsIndex < 0 || bucketStart > prevBucketStart) {
      IntStream.range(0, 2).forEach(idx -> {
        timestamps.add(timestamp);
        dataPoints.add(data);
      });
    } else if (bucketStart == prevBucketStart) {
      long currentMinTimestamp = timestamps.getLong(currMaxTsIndex - 1);
      long currentMaxTimestamp = timestamps.getLong(currMaxTsIndex);
      if (timestamp < currentMinTimestamp) {
        timestamps.set(currMaxTsIndex - 1, timestamp);
        dataPoints.set(currMaxTsIndex - 1, data);
      }
      if (timestamp > currentMaxTimestamp) {
        timestamps.set(currMaxTsIndex, timestamp);
        dataPoints.set(currMaxTsIndex, data);
      }
    } else {
      throw new RE("DeltaTimeseries data is not sorted." + "Found bucket start %d after %d (timestamp : %d)",
                   bucketStart,
                   prevBucketStart,
                   timestamp);
    }
  }

  @Override
  protected void internalMergeSeries(List<DeltaTimeSeries> mergeSeries)
  {
    if (mergeSeries.isEmpty()) {
      return;
    }

    mergeSeries.forEach(DeltaTimeSeries::build);
    ImplyLongArrayList mergedTimestamps = new ImplyLongArrayList();
    ImplyDoubleArrayList mergedDataPoints = new ImplyDoubleArrayList();
    Iterator<BucketData> mergedTimeSeries = Iterators.mergeSorted(mergeSeries.stream()
                                                                             .map(DeltaTimeSeries::getIterator)
                                                                             .collect(Collectors.toList()),
                                                                  Comparator.comparingLong(BucketData::getMinTs));

    int currIndex = -1;
    long prevBucketStart = -1;
    while (mergedTimeSeries.hasNext()) {
      BucketData deltaSeriesEntry = mergedTimeSeries.next();
      long bucketStart = timeBucketGranularity.bucketStart(deltaSeriesEntry.getMinTs());
      if (currIndex == -1 || (bucketStart > prevBucketStart)) {
        mergedTimestamps.add(deltaSeriesEntry.getMinTs());
        mergedTimestamps.add(deltaSeriesEntry.getMaxTs());
        mergedDataPoints.add(deltaSeriesEntry.getMinDataPoint());
        mergedDataPoints.add(deltaSeriesEntry.getMaxDataPoint());
        currIndex++;
        prevBucketStart = timeBucketGranularity.bucketStart(deltaSeriesEntry.getMinTs());
      } else if (bucketStart == prevBucketStart) {
        long currentMinTimestamp = mergedTimestamps.getLong(2 * currIndex);
        long currentMaxTimestamp = mergedTimestamps.getLong(2 * currIndex + 1);
        if (deltaSeriesEntry.getMinTs() < currentMinTimestamp) {
          mergedTimestamps.set(2 * currIndex, deltaSeriesEntry.getMinTs());
          mergedDataPoints.set(2 * currIndex, deltaSeriesEntry.getMinDataPoint());
        }
        if (deltaSeriesEntry.getMaxTs() > currentMaxTimestamp) {
          mergedTimestamps.set(2 * currIndex + 1, deltaSeriesEntry.getMaxTs());
          mergedDataPoints.set(2 * currIndex + 1, deltaSeriesEntry.getMaxDataPoint());
        }
      } else {
        throw new RE("DeltaTimeseries data is not sorted." + "Found bucket start %d after %d (timestamp : %d)",
                     bucketStart,
                     prevBucketStart,
                     deltaSeriesEntry.getMinTs());
      }
    }
    DeltaTimeSeries mergedSeries = new DeltaTimeSeries(mergedTimestamps,
                                                       mergedDataPoints,
                                                       getTimeBucketGranularity(),
                                                       getwindow(),
                                                       getStart(),
                                                       getEnd(),
                                                       getMaxEntries());
    copy(mergedSeries);
  }

  @Override
  public void addTimeSeries(DeltaTimeSeries timeSeries)
  {
    boolean compatibleMerge = timeSeries.getTimeBucketGranularity().equals(getTimeBucketGranularity());
    if (!compatibleMerge) {
      throw new IAE("The time series to merge are incompatible. Trying to merge %s granularity into %s",
                    timeSeries.getTimeBucketGranularity(),
                    getTimeBucketGranularity());
    }
    timeSeriesList.add(timeSeries);
  }

  @Override
  public void build()
  {
    if (timeSeriesList.isEmpty()) {
      return;
    }
    List<DeltaTimeSeries> mergeList = new ArrayList<>(timeSeriesList);
    timeSeriesList.clear();
    mergeList.add(this);
    mergeSeries(mergeList);
  }

  @Override
  public SimpleTimeSeries computeSimple()
  {
    build();
    int simpleSeriesSize = timestamps.size() / 2;
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(simpleSeriesSize),
                                                             new ImplyDoubleArrayList(simpleSeriesSize),
                                                             getwindow(),
                                                             getStart(),
                                                             getEnd(),
                                                             getMaxEntries());
    for (int i = 0; i < simpleSeriesSize; i++) {
      simpleTimeSeries.addDataPoint(timeBucketGranularity.bucketStart(timestamps.getLong(2 * i)),
                                    dataPoints.getDouble(2 * i + 1) - dataPoints.getDouble(2 * i));
    }
    simpleTimeSeries.build();
    return simpleTimeSeries;
  }

  @Override
  protected void internalCopy(DeltaTimeSeries copySeries)
  {
    this.timestamps = copySeries.getTimestamps();
    this.dataPoints = copySeries.getDataPoints();
    this.timeSeriesList = copySeries.getTimeSeriesList();
    this.timeBucketGranularity = copySeries.getTimeBucketGranularity();
  }

  private static class BucketData
  {
    private final long minTs;
    private final long maxTs;
    private final double minDataPoint;
    private final double maxDataPoint;

    public BucketData(long minTs, long maxTs, double minDataPoint, double maxDataPoint)
    {
      this.minTs = minTs;
      this.maxTs = maxTs;
      this.minDataPoint = minDataPoint;
      this.maxDataPoint = maxDataPoint;
    }

    public long getMinTs()
    {
      return minTs;
    }

    public long getMaxTs()
    {
      return maxTs;
    }

    public double getMinDataPoint()
    {
      return minDataPoint;
    }

    public double getMaxDataPoint()
    {
      return maxDataPoint;
    }
  }
}
