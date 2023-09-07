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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

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

  @JsonProperty
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
  
  public void addTimeSeries(DownsampledSumTimeSeries mergeSeries)
  {
    boolean compatibleMerge = mergeSeries.getTimeBucketGranularity().equals(getTimeBucketGranularity());
    if (!compatibleMerge) {
      throw new IAE(
          "The time series to merge are incompatible. Trying to merge %s granularity into %s",
          mergeSeries.getTimeBucketGranularity(),
          getTimeBucketGranularity()
      );
    }
    if (!getWindow().equals(mergeSeries.getWindow())) {
      throw DruidException.defensive(
          "The time series to merge have different visible windows : (%s, %s)",
          getWindow(),
          mergeSeries.getWindow()
      );
    }

    // merge the timestamps that are same and collected the ones in the merge series that aren't same
    int currSeriescounter = 0, mergeSeriesCounter = 0;
    IntArrayList leftoverPoints = new IntArrayList();

    ImplyDoubleArrayList dataPoints = getDataPoints();
    ImplyDoubleArrayList mergeSeriesDataPoints = mergeSeries.getDataPoints();
    ImplyLongArrayList timestamps = getTimestamps();
    ImplyLongArrayList mergeSeriesTimestamps = mergeSeries.getTimestamps();
    while (mergeSeriesCounter < mergeSeries.size() && currSeriescounter < size()) {
      long mergeSeriesTimestamp = mergeSeriesTimestamps.getLong(mergeSeriesCounter);
      long currSeriesTimestamp = timestamps.getLong(currSeriescounter);
      if (mergeSeriesTimestamp == currSeriesTimestamp) {
        dataPoints.set(
            currSeriescounter,
            dataPoints.getDouble(currSeriescounter) + mergeSeriesDataPoints.getDouble(mergeSeriesCounter)
        );
        mergeSeriesCounter++;
        currSeriescounter++;
      } else if (mergeSeriesTimestamp < currSeriesTimestamp) {
        leftoverPoints.add(mergeSeriesCounter);
        mergeSeriesCounter++;
      } else {
        currSeriescounter++;
      }
    }
    while (mergeSeriesCounter < mergeSeries.size()) {
      leftoverPoints.add(mergeSeriesCounter);
      mergeSeriesCounter++;
    }

    // resize the current series to adjust for leftover points
    int oldSize = size();
    int newSize = oldSize + leftoverPoints.size();
    timestamps.size(newSize);
    dataPoints.size(newSize);

    // put the leftover points to their correct place
    int oldSizeCounter = oldSize - 1;
    int leftOverCounter = leftoverPoints.size() - 1;
    int newSeriesCounter = newSize - 1;
    while (oldSizeCounter >= 0 && leftOverCounter >= 0) {
      long currSeriesTimestamp = timestamps.getLong(oldSizeCounter);
      long mergeSeriesTimestamp = mergeSeriesTimestamps.getLong(leftoverPoints.getInt(leftOverCounter));
      if (currSeriesTimestamp < mergeSeriesTimestamp) {
        timestamps.set(newSeriesCounter, mergeSeriesTimestamp);
        dataPoints.set(newSeriesCounter, mergeSeriesDataPoints.getDouble(leftoverPoints.getInt(leftOverCounter)));
        newSeriesCounter--;
        leftOverCounter--;
      } else if (currSeriesTimestamp > mergeSeriesTimestamp) {
        timestamps.set(newSeriesCounter, currSeriesTimestamp);
        dataPoints.set(newSeriesCounter, dataPoints.getDouble(oldSizeCounter));
        newSeriesCounter--;
        oldSizeCounter--;
      } else {
        throw DruidException.defensive("");
      }
    }
    while (leftOverCounter >= 0) {
      timestamps.set(newSeriesCounter, mergeSeriesTimestamps.getLong(leftoverPoints.getInt(leftOverCounter)));
      dataPoints.set(newSeriesCounter, mergeSeriesDataPoints.getDouble(leftoverPoints.getInt(leftOverCounter)));
      newSeriesCounter--;
      leftOverCounter--;
    }

    final EdgePoint startBoundary = mergeSeries.getStart();
    if (startBoundary != null && startBoundary.getTimestampJson() != null) {
      addDataPoint(startBoundary.getTimestamp(), startBoundary.getData());
    }

    final EdgePoint endBoundary = mergeSeries.getEnd();
    if (endBoundary != null && endBoundary.getTimestampJson() != null) {
      addDataPoint(endBoundary.getTimestamp(), endBoundary.getData());
    }
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
