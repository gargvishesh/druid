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
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndexColumnSelectorFactory;
import org.apache.druid.segment.QueryableIndexCursorSequenceBuilder;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    final int columnNumber = frameReader.signature().indexOf(column);

    if (columnNumber < 0) {
      return null;
    } else {
      // Better than frameReader.frameSignature().getColumnCapabilities(column), because this method has more
      // insight into what's actually going on with this column (nulls, multivalue, etc).
      return frameReader.columnReaders().get(columnNumber).readColumn(frame).getCapabilities();
    }
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
  public boolean canVectorize(@Nullable Filter filter, VirtualColumns virtualColumns, boolean descending)
  {
    // TODO(gianm): impl vectorized cursor enough that it's actually usable
    return false;
  }

  @Nullable
  private static Filter buildFilter(@Nullable Filter filter, Interval interval)
  {
    if (Intervals.ETERNITY.equals(interval)) {
      return filter;
    } else {
      return Filters.and(
          Arrays.asList(
              new BoundFilter(
                  new BoundDimFilter(
                      ColumnHolder.TIME_COLUMN_NAME,
                      String.valueOf(interval.getStartMillis()),
                      String.valueOf(interval.getEndMillis()),
                      false,
                      true,
                      null,
                      null,
                      StringComparators.NUMERIC
                  )
              ),
              filter
          )
      );
    }
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
    final FrameColumnSelector frameColumnSelector = new FrameColumnSelector(frame, frameReader);

    if (Granularities.ALL.equals(gran)) {
      final Cursor cursor = makeGranularityAllCursor(frameColumnSelector, filter, interval, virtualColumns, descending);

      // Note: if anything closeable is ever added to this Sequence, make sure to update FrameProcessors.makeCursor.
      // Currently, it assumes that closing the Sequence does nothing.
      return Sequences.simple(Collections.singletonList(cursor));
    } else {
      // If gran != ALL, assume the frame is time-ordered. Callers shouldn't use gran != ALL on non-time-ordered data.
      // TODO(gianm): Validate the above, instead of assuming it.

      if (frameColumnSelector.getNumRows() == 0) {
        return Sequences.empty();
      }

      final ColumnHolder timeColumnHolder = frameColumnSelector.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME);
      final long firstTime, lastTime;

      if (timeColumnHolder == null) {
        firstTime = 0;
        lastTime = 0;
      } else {
        final NumericColumn timeColumn = (NumericColumn) timeColumnHolder.getColumn();
        firstTime = timeColumn.getLongSingleValueRow(0);
        lastTime = timeColumn.getLongSingleValueRow(frameColumnSelector.getNumRows() - 1);
      }

      final Interval intervalToUse = interval.overlap(Intervals.utc(firstTime, lastTime + 1));

      if (intervalToUse == null) {
        return Sequences.empty();
      }

      return new QueryableIndexCursorSequenceBuilder(
          frameColumnSelector,
          intervalToUse,
          virtualColumns,
          null,
          firstTime,
          lastTime,
          descending,
          filter,
          null
      ).build(gran);
    }
  }

  @Nullable
  @Override
  public VectorCursor makeVectorCursor(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    if (!canVectorize(filter, virtualColumns, descending)) {
      throw new ISE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor'.");
    }

    final FrameColumnSelector columnSelector = new FrameColumnSelector(frame, frameReader);
    final Closer closer = Closer.create();
    final Filter filterToUse = buildFilter(filter, interval);
    final VectorOffset baseOffset = new NoFilterVectorOffset(vectorSize, 0, frame.numRows());
    final Map<String, BaseColumn> columnCache = new HashMap<>();

    // baseColumnSelectorFactory using baseOffset is the column selector for filtering.
    final VectorColumnSelectorFactory baseColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
        columnSelector,
        baseOffset,
        closer,
        columnCache,
        virtualColumns
    );

    if (filterToUse == null) {
      return new FrameVectorCursor(frame, frameReader, baseOffset, baseColumnSelectorFactory, closer);
    } else {
      final FilteredVectorOffset filteredOffset = FilteredVectorOffset.create(
          baseOffset,
          baseColumnSelectorFactory,
          filterToUse
      );

      final VectorColumnSelectorFactory filteredColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
          columnSelector,
          filteredOffset,
          closer,
          columnCache,
          virtualColumns
      );

      return new FrameVectorCursor(frame, frameReader, filteredOffset, filteredColumnSelectorFactory, closer);
    }
  }

  private static Cursor makeGranularityAllCursor(
      final FrameColumnSelector frameColumnSelector,
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final boolean descending
  )
  {
    final Filter filterToUse = buildFilter(filter, interval);

    final Offset baseOffset = descending
                              ? new SimpleDescendingOffset(frameColumnSelector.getNumRows())
                              : new SimpleAscendingOffset(frameColumnSelector.getNumRows());

    final Offset offset;

    final QueryableIndexColumnSelectorFactory columnSelectorFactory =
        new QueryableIndexColumnSelectorFactory(
            frameColumnSelector,
            virtualColumns,
            false,
            Closer.create(),
            baseOffset,
            new HashMap<>()
        );

    if (filterToUse == null) {
      offset = baseOffset;
    } else {
      offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
    }

    return new FrameCursor(offset, columnSelectorFactory);
  }
}
