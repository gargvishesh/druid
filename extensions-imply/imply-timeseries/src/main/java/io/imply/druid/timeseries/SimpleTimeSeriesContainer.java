/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class SimpleTimeSeriesContainer
{
  private final boolean isBuffered;

  private SimpleTimeSeriesBuffer simpleTimeSeriesBuffer;
  private SimpleTimeSeries simpleTimeSeries;

  private SimpleTimeSeriesContainer(SimpleTimeSeriesBuffer simpleTimeSeriesBuffer)
  {
    this.simpleTimeSeriesBuffer = simpleTimeSeriesBuffer;
    isBuffered = true;
  }

  private SimpleTimeSeriesContainer(@Nullable SimpleTimeSeries simpleTimeSeries)
  {
    this.simpleTimeSeries = simpleTimeSeries;
    isBuffered = false;
  }

  public static SimpleTimeSeriesContainer createFromBuffer(SimpleTimeSeriesBuffer timeSeriesBuffer)
  {
    return new SimpleTimeSeriesContainer(timeSeriesBuffer);
  }

  public static SimpleTimeSeriesContainer createFromInstance(SimpleTimeSeries simpleTimeSeries)
  {
    return new SimpleTimeSeriesContainer(simpleTimeSeries);
  }

  @JsonValue
  public SimpleTimeSeries getSimpleTimeSeries()
  {
    return isBuffered ? simpleTimeSeriesBuffer.getSimpleTimeSeries() : simpleTimeSeries;
  }

  public SimpleTimeSeriesData getSimpleTimeSeriesData()
  {
    return getSimpleTimeSeries().asSimpleTimeSeriesData();
  }

  public SimpleTimeSeries computeSimple()
  {
    return getSimpleTimeSeries().computeSimple();
  }

  public void pushInto(SimpleTimeSeries simpleTimeSeries)
  {
    SimpleTimeSeries thisSimpleTimeSeries = getSimpleTimeSeries();
    if (thisSimpleTimeSeries != null) {
      ImplyLongArrayList timestamps = thisSimpleTimeSeries.getTimestamps();
      ImplyDoubleArrayList datapoints = thisSimpleTimeSeries.getDataPoints();

      for (int i = 0; i < timestamps.size(); i++) {
        simpleTimeSeries.addDataPoint(timestamps.getLong(i), datapoints.getDouble(i));
      }
    }
  }

  public void pushInto(
      SimpleByteBufferTimeSeries simpleByteBufferTimeSeries,
      WritableMemory writableMemory,
      int pos,
      Interval window
  )
  {
    // apply query window to properly filter data points and compute edges
    SimpleTimeSeries windowFilteredTimeSeries = getSimpleTimeSeries().withWindow(window);

    simpleByteBufferTimeSeries.mergeSeriesBuffered(writableMemory, pos, windowFilteredTimeSeries);
  }

  public void pushInto(SimpleTimeSeriesContainer simpleTimeSeriesContainer)
  {
    pushInto(simpleTimeSeriesContainer.getSimpleTimeSeries());
  }

  public boolean isNull()
  {
    return isBuffered ? simpleTimeSeriesBuffer.isNull() : (simpleTimeSeries == null);
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
    SimpleTimeSeriesContainer that = (SimpleTimeSeriesContainer) o;
    return Objects.equal(getSimpleTimeSeries(), that.getSimpleTimeSeries());
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(getSimpleTimeSeries());
  }
}
