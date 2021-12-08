/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment;

import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameColumnReader;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link org.apache.druid.segment.ColumnSelector} implementation based on a single frame.
 *
 * Implements {@link QueryableIndex} too, so it can be used in places that expect {@link QueryableIndex} in some
 * cases. However, note that not all {@link QueryableIndex} methods are implemented, so this is not always going to be
 * a perfect replacement.
 *
 * In a future where {@link org.apache.druid.segment.QueryableIndexColumnSelectorFactory} is modified to accept
 * {@link org.apache.druid.segment.ColumnSelector} instead of {@link QueryableIndex}, this class could be changed to
 * implement {@link org.apache.druid.segment.ColumnSelector} directly.
 *
 * Not thread-safe.
 */
public class FrameColumnSelector implements QueryableIndex
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final Map<String, ColumnHolder> columnCache = new HashMap<>();

  public FrameColumnSelector(Frame frame, FrameReader frameReader)
  {
    this.frame = frame;
    this.frameReader = frameReader;
  }

  @Override
  public int getNumRows()
  {
    return frame.numRows();
  }

  @Override
  public List<String> getColumnNames()
  {
    return frameReader.signature().getColumnNames();
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(final String columnName)
  {
    return columnCache.computeIfAbsent(
        columnName,
        c -> {
          final int columnIndex = frameReader.signature().indexOf(columnName);

          if (columnIndex < 0) {
            return null;
          } else {
            final FrameColumnReader columnReader = frameReader.columnReaders().get(columnIndex);
            return columnReader.readColumn(frame);
          }
        }
    );
  }

  @Override
  public Interval getDataInterval()
  {
    // No interval is known ahead of time for frames. It is not expected that this method will actually be used.
    throw new UnsupportedOperationException("No data interval");
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(frameReader.signature().getColumnNames());
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    // No bitmaps for frames. It is not expected that this method will actually be used.
    throw new UnsupportedOperationException("No bitmap factory");
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return null;
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return Collections.emptyMap();
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
