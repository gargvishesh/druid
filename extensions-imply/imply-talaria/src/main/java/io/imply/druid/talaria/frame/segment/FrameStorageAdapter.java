/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * A {@link StorageAdapter} implementation based on a single {@link Frame}.
 *
 * This class is used for both columnar and row-based frames.
 */
public class FrameStorageAdapter implements StorageAdapter
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final Interval interval;

  public FrameStorageAdapter(Frame frame, FrameReader frameReader, Interval interval)
  {
    this.frame = frame;
    this.frameReader = frameReader;
    this.interval = interval;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(frameReader.signature().getColumnNames());
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Collections.emptyList();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    // Unknown, so return Integer.MAX_VALUE.
    // TODO(gianm): Is this OK? It's what the interface javadocs say to do, but we should double-check usage.
    //   May make more sense to switch the interface to ask for CARDINALITY_UNKNOWN
    return Integer.MAX_VALUE;
  }

  @Override
  public DateTime getMinTime()
  {
    return getInterval().getStart();
  }

  @Override
  public DateTime getMaxTime()
  {
    return getInterval().getEnd().minus(1);
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    // It's ok to return null always, because callers are required to handle the case where the min value is not known.
    // TODO(gianm): Add notes about this to javadocs
    return null;
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    // It's ok to return null always, because callers are required to handle the case where the max value is not known.
    // TODO(gianm): Add notes about this to javadocs
    return null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return frameReader.columnCapabilities(frame, column);
  }

  @Override
  public int getNumRows()
  {
    return frame.numRows();
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return getMaxTime();
  }

  @Override
  public Metadata getMetadata()
  {
    throw new UnsupportedOperationException("Cannot retrieve metadata");
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    return frameReader.makeCursorFactory(frame).makeCursors(
        filter,
        interval,
        virtualColumns,
        gran,
        descending,
        queryMetrics
    );
  }
}
