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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.collect.Utils;
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
 * This maintains an intermediate TS for tracking the mean of all samples present per time bucket.
 * The time buckets are created on fly on the basis of the bucket duration as the data is read.
 */
public class DownsampledSumTimeSeries extends TimeSeries<DownsampledSumTimeSeries>
{
  private ImplyLongArrayList bucketStarts;
  private ImplyDoubleArrayList sumPoints;
  private DurationGranularity timeBucketGranularity;

  public DownsampledSumTimeSeries(DurationGranularity timeBucketGranularity, Interval window, int maxEntries)
  {
    this(
        new ImplyLongArrayList(),
        new ImplyDoubleArrayList(),
        timeBucketGranularity,
        window,
        null,
        null,
        maxEntries
    );
  }

  public DownsampledSumTimeSeries(
      ImplyLongArrayList bucketStarts,
      ImplyDoubleArrayList sumPoints,
      DurationGranularity timeBucketGranularity,
      Interval window,
      @Nullable EdgePoint start,
      @Nullable EdgePoint end,
      int maxEntries
  )
  {
    super(window, start, end, maxEntries);
    this.bucketStarts = bucketStarts;
    this.sumPoints = sumPoints;
    this.timeBucketGranularity = Objects.requireNonNull(timeBucketGranularity, "Must have a non-null duration");
  }

  private Iterator<BucketData> getIterator()
  {
    return IntStream.range(0, bucketStarts.size())
                              .mapToObj(idx -> new BucketData(bucketStarts.getLong(idx), sumPoints.getDouble(idx)))
                              .iterator();
  }

  @JsonProperty
  public ImplyDoubleArrayList getSumPoints()
  {
    return sumPoints;
  }

  @JsonProperty
  public ImplyLongArrayList getBucketStarts()
  {
    return bucketStarts;
  }

  @JsonProperty
  public DurationGranularity getTimeBucketGranularity()
  {
    return new DurationGranularity(timeBucketGranularity.getDuration(), timeBucketGranularity.getOrigin());
  }

  public List<DownsampledSumTimeSeries> getTimeSeriesList()
  {
    return timeSeriesList;
  }

  @Override
  public int size()
  {
    build();
    return bucketStarts.size() + timeSeriesList.stream().mapToInt(DownsampledSumTimeSeries::size).sum();
  }

  @Override
  protected void internalAddDataPoint(long timestamp, double data)
  {
    long bucketStart = timeBucketGranularity.bucketStart(timestamp);
    int currIndex = bucketStarts.size() - 1;
    if (currIndex < 0 || bucketStart > bucketStarts.getLong(currIndex)) {
      bucketStarts.add(bucketStart);
      sumPoints.add(data);
    } else if (bucketStart == bucketStarts.getLong(currIndex)) {
      sumPoints.set(currIndex, sumPoints.getDouble(currIndex) + data);
    } else {
      throw new RE("MeanTimeseries data is not sorted." + "Found bucket start %d after %d (timestamp : %d)",
                   bucketStart,
                   bucketStarts.getLong(currIndex),
                   timestamp);
    }
  }

  @Override
  protected void internalMergeSeries(List<DownsampledSumTimeSeries> mergeSeries)
  {
    if (mergeSeries.isEmpty()) {
      return;
    }

    mergeSeries.forEach(DownsampledSumTimeSeries::build);
    ImplyLongArrayList mergedBucketStarts = new ImplyLongArrayList();
    ImplyDoubleArrayList mergedSumPoints = new ImplyDoubleArrayList();
    Iterator<BucketData> mergedTimeSeries = Utils.mergeSorted(
        mergeSeries.stream().map(DownsampledSumTimeSeries::getIterator).collect(Collectors.toList()),
        Comparator.comparingLong(BucketData::getStart)
    );
    int currIndex = -1;
    while (mergedTimeSeries.hasNext()) {
      BucketData meanSeriesEntry = mergedTimeSeries.next();
      if (currIndex == -1 || (meanSeriesEntry.getStart() > mergedBucketStarts.getLong(currIndex))) {
        mergedBucketStarts.add(meanSeriesEntry.getStart());
        mergedSumPoints.add(meanSeriesEntry.getSum());
        currIndex++;
      } else if (meanSeriesEntry.getStart() == mergedBucketStarts.getLong(currIndex)) {
        mergedSumPoints.set(currIndex, mergedSumPoints.getDouble(currIndex) + meanSeriesEntry.getSum());
      } else {
        throw new RE("MeanTimeseries data is not sorted." + "Found bucket start %d after %d",
                     meanSeriesEntry.getStart(),
                     mergedBucketStarts.getLong(currIndex));
      }
    }
    DownsampledSumTimeSeries mergedSeries = new DownsampledSumTimeSeries(
        mergedBucketStarts,
        mergedSumPoints,
        getTimeBucketGranularity(),
        getWindow(),
        getStart(),
        getEnd(),
        getMaxEntries()
    );
    copy(mergedSeries);
  }

  @Override
  public void addTimeSeries(DownsampledSumTimeSeries timeSeries)
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
    List<DownsampledSumTimeSeries> mergeList = new ArrayList<>(timeSeriesList);
    timeSeriesList.clear();
    mergeList.add(this);
    mergeSeries(mergeList);
  }

  @Override
  public SimpleTimeSeries computeSimple()
  {
    build();
    int simpleSeriesSize = bucketStarts.size();
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(simpleSeriesSize),
        new ImplyDoubleArrayList(simpleSeriesSize),
        getWindow(),
        getStart(),
        getEnd(),
        getMaxEntries(),
        1L
    );
    for (int i = 0; i < simpleSeriesSize; i++) {
      simpleTimeSeries.addDataPoint(bucketStarts.getLong(i), sumPoints.getDouble(i));
    }
    simpleTimeSeries.build();
    return simpleTimeSeries;
  }

  @Override
  protected void internalCopy(DownsampledSumTimeSeries copySeries)
  {
    this.bucketStarts = copySeries.getBucketStarts();
    this.sumPoints = copySeries.getSumPoints();
    this.timeSeriesList = copySeries.getTimeSeriesList();
    this.timeBucketGranularity = copySeries.getTimeBucketGranularity();
  }

  private static class BucketData
  {
    private final long start;
    private final double sum;

    public BucketData(long start, double sum)
    {
      this.start = start;
      this.sum = sum;
    }

    public long getStart()
    {
      return start;
    }

    public double getSum()
    {
      return sum;
    }
  }
}