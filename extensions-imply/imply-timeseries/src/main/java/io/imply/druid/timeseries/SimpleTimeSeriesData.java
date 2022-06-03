/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.google.common.base.Objects;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.joda.time.Interval;

public class SimpleTimeSeriesData
{
  private final ImplyLongArrayList timestamps;
  private final ImplyDoubleArrayList dataPoints;
  private final Interval window;

  public SimpleTimeSeriesData(ImplyLongArrayList timestamps, ImplyDoubleArrayList dataPoints, Interval window)
  {
    this.timestamps = timestamps;
    this.dataPoints = dataPoints;
    this.window = window;
  }

  public ImplyLongArrayList getTimestamps()
  {
    return timestamps;
  }

  public ImplyDoubleArrayList getDataPoints()
  {
    return dataPoints;
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
    SimpleTimeSeriesData that = (SimpleTimeSeriesData) o;
    return Objects.equal(timestamps, that.timestamps) && Objects.equal(
        dataPoints,
        that.dataPoints
    ) && Objects.equal(window, that.window);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(timestamps, dataPoints, window);
  }


  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("timestamps", timestamps)
                  .add("dataPoints", dataPoints)
                  .add("window", window)
                  .toString();
  }
}
