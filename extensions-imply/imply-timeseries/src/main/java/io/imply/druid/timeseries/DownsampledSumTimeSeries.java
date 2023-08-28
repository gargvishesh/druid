/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This maintains an intermediate TS for tracking the mean of all samples present per time bucket.
 * The time buckets are created on fly on the basis of the bucket duration as the data is read.
 */
public class DownsampledSumTimeSeries extends SimpleTimeSeries
{
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
      ImplyLongArrayList timestamps,
      ImplyDoubleArrayList dataPoints,
      DurationGranularity timeBucketGranularity,
      Interval window,
      @Nullable TimeSeries.EdgePoint start,
      @Nullable TimeSeries.EdgePoint end,
      int maxEntries
  )
  {
    super(
        timestamps,
        dataPoints,
        window,
        start,
        end,
        maxEntries,
        timeBucketGranularity.getDurationMillis()
    );
    this.timeBucketGranularity = Objects.requireNonNull(timeBucketGranularity, "Must have a non-null duration");
  }

  public DurationGranularity getTimeBucketGranularity()
  {
    return timeBucketGranularity;
  }

  @Override
  protected void internalAddDataPoint(long timestamp, double data)
  {
    long bucketStart = timeBucketGranularity.bucketStart(timestamp);
    int currIndex = getTimestamps().size() - 1;
    if (currIndex < 0 || bucketStart > getTimestamps().getLong(currIndex)) {
      getTimestamps().add(bucketStart);
      getDataPoints().add(data);
    } else if (bucketStart == getTimestamps().getLong(currIndex)) {
      getDataPoints().set(currIndex, getDataPoints().getDouble(currIndex) + data);
    } else {
      throw new RE("DownsampledSumTimeSeries data is not sorted." + "Found bucket start [%d] after [%d] (timestamp : [%d])",
                   bucketStart,
                   getTimestamps().getLong(currIndex),
                   timestamp);
    }
  }

  @Override
  protected void internalMergeSeries(List<SimpleTimeSeries> mergeSeries)
  {
    if (mergeSeries.isEmpty()) {
      return;
    }

    mergeSeries.forEach(SimpleTimeSeries::build);
    ImplyLongArrayList mergedBucketStarts = new ImplyLongArrayList();
    ImplyDoubleArrayList mergedSumPoints = new ImplyDoubleArrayList();
    Iterator<Pair<Long, Double>> mergedTimeSeries = Utils.mergeSorted(
        mergeSeries.stream().map(SimpleTimeSeries::getIterator).collect(Collectors.toList()),
        Comparator.comparingLong(lhs -> lhs.lhs)
    );
    int currIndex = -1;
    while (mergedTimeSeries.hasNext()) {
      Pair<Long, Double> meanSeriesEntry = mergedTimeSeries.next();
      if (currIndex == -1 || (meanSeriesEntry.lhs > mergedBucketStarts.getLong(currIndex))) {
        mergedBucketStarts.add(meanSeriesEntry.lhs.longValue());
        mergedSumPoints.add(meanSeriesEntry.rhs.longValue());
        currIndex++;
      } else if (meanSeriesEntry.lhs == mergedBucketStarts.getLong(currIndex)) {
        mergedSumPoints.set(currIndex, mergedSumPoints.getDouble(currIndex) + meanSeriesEntry.rhs);
      } else {
        throw new RE("DownsampledSumTimeSeries data is not sorted." + "Found bucket start [%d] after [%d]",
                     meanSeriesEntry.lhs,
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
  public void addTimeSeries(SimpleTimeSeries timeSeries)
  {
    boolean compatibleMerge = timeSeries.getBucketMillis().equals(getTimeBucketGranularity().getDurationMillis());
    if (!compatibleMerge) {
      throw new IAE(
          "The time series to merge are incompatible. Trying to merge [%s] bucket millis into [%s]",
          getTimeBucketGranularity().getDurationMillis(),
          timeSeries.getBucketMillis()
      );
    }
    timeSeriesList.add(timeSeries);
  }

  @Override
  public SimpleTimeSeries computeSimple()
  {
    SimpleTimeSeries simpleTimeSeries = super.computeSimple();
    return new SimpleTimeSeries(
      simpleTimeSeries.getTimestamps(),
      simpleTimeSeries.getDataPoints(),
      simpleTimeSeries.getWindow(),
      simpleTimeSeries.getStart(),
      simpleTimeSeries.getEnd(),
      simpleTimeSeries.getMaxEntries(),
      simpleTimeSeries.getBucketMillis()
    );
  }

  @Override
  protected void internalCopy(SimpleTimeSeries copySeries)
  {
    super.internalCopy(copySeries);
    this.timeBucketGranularity = new DurationGranularity(copySeries.getBucketMillis(), 0);
  }
}
