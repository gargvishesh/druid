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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.guava.GuavaUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for a time series. It provides an way to build a time series by adding data points, existing time series or
 * using a mix of both. The object is mutable and is not thread safe.
 * @param <T>
 */
public abstract class TimeSeries<T extends TimeSeries<T>>
{
  private Interval window;
  private EdgePoint start;
  private EdgePoint end;
  private final int maxEntries;

  public TimeSeries(Interval window,
                    @Nullable EdgePoint start,
                    @Nullable EdgePoint end,
                    int maxEntries)
  {
    this.window = Preconditions.checkNotNull(window, "window is null");
    this.start = GuavaUtils.firstNonNull(start, empty());
    this.end = GuavaUtils.firstNonNull(end, empty());
    this.maxEntries = maxEntries;
  }

  /**
   * Public interface to add a data point to the TS. This only passes the relevant data points, ie the ones in visible window
   * (if any) to the derived object. It also maintains the bounds of the visible window.
   * @param timestamp
   * @param data
   */
  public void addDataPoint(long timestamp, double data)
  {
    // if the timestamp is in visible window (if any), then add it. Otherwise update the edges if needed
    if (window.contains(timestamp)) {
      internalAddDataPoint(timestamp, data);
    } else {
      if (timestamp < window.getStartMillis()) {
        if (start.timestamp < timestamp || start.timestamp == -1) {
          start.setTimestamp(timestamp);
          start.setData(data);
        }
      } else if (timestamp >= window.getEndMillis()) {
        if (end.timestamp > timestamp || end.timestamp == -1) {
          end.setTimestamp(timestamp);
          end.setData(data);
        }
      }
    }
  }

  /**
   * Public interface to copy the attributes of the argument to itself
   * @param copySeries
   */
  public void copy(T copySeries)
  {
    // copy the base
    this.window = copySeries.getWindow();
    this.start = copySeries.getStart();
    this.end = copySeries.getEnd();

    // copy dervied object
    internalCopy(copySeries);
  }

  @JsonProperty
  private Map<String, EdgePoint> getBounds()
  {
    return ImmutableMap.of("start", start, "end", end);
  }

  @JsonProperty
  public Interval getWindow()
  {
    return window;
  }

  public EdgePoint getStart()
  {
    return start;
  }

  public EdgePoint getEnd()
  {
    return end;
  }

  public int getMaxEntries()
  {
    return maxEntries;
  }

  private static EdgePoint empty()
  {
    return new EdgePoint(-1, -1);
  }

  /**
   * Add data point to the derived object. The data point will be in the visible window (if any) of this TS.
   * @param timestamp
   * @param data
   */
  protected abstract void internalAddDataPoint(long timestamp, double data);

  /**
   * Add time series to the derived object. The time series will have the same visible window (if any) as of this TS.
   * @param timeSeries
   */
  abstract void addTimeSeries(T timeSeries);

  /**
   * Converts an intermediate TS to a final time series. This can trigger a build followed by the final computation.
   * @return converted final simple time series
   */
  public abstract SimpleTimeSeries computeSimple();

  abstract int size();

  /**
   * Copies the attributes of the argument TS to itself.
   * @param copySeries
   */
  protected abstract void internalCopy(T copySeries);

  @Override
  public int hashCode()
  {
    return Objects.hash(window, start, end, maxEntries);
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
    TimeSeries<?> that = (TimeSeries<?>) o;
    return Objects.equals(window, that.getWindow()) &&
           Objects.equals(start, that.getStart()) &&
           Objects.equals(end, that.getEnd()) &&
           Objects.equals(maxEntries, that.maxEntries);
  }

  @Override
  public String toString()
  {
    return "TimeSeries{" +
           "window=" + window +
           ", start=" + start +
           ", end=" + end +
           ", maxEntries=" + maxEntries +
           '}';
  }

  /**
   * Holds the edges / boundaries for the timeseries. The boundaries are defined only if a visible window is present.
   */
  public static class EdgePoint
  {
    private long timestamp;
    public double data;

    public EdgePoint(long timestamp, double data)
    {
      this.timestamp = timestamp;
      this.data = data;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    @Nullable
    @JsonProperty("timestamp")
    public Long getTimestampJson()
    {
      return timestamp == -1 ? null : timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }

    public Double getData()
    {
      return data;
    }

    @Nullable
    @JsonProperty("data")
    public Double getDataJson()
    {
      return data == -1 ? null : data;
    }

    public void setData(double data)
    {
      this.data = data;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(timestamp, data);
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
      EdgePoint that = (EdgePoint) o;
      return Objects.equals(timestamp, that.getTimestamp()) &&
             Objects.equals(data, that.getData());
    }

    @Override
    public String toString()
    {
      return "EdgePoint{" +
             "timestamp=" + timestamp +
             ", data=" + data +
             '}';
    }
  }
}
