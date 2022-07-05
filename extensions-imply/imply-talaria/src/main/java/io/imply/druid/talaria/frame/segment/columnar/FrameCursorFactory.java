/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.columnar;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.read.columnar.FrameColumnReader;
import io.imply.druid.talaria.frame.segment.FrameCursor;
import io.imply.druid.talaria.frame.segment.FrameCursorUtils;
import io.imply.druid.talaria.frame.segment.FrameFilteredOffset;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndexColumnSelectorFactory;
import org.apache.druid.segment.QueryableIndexCursorSequenceBuilder;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@link CursorFactory} implementation based on a single columnar {@link Frame}.
 *
 * This class is only used for columnar frames. It is not used for row-based frames.
 */
public class FrameCursorFactory implements CursorFactory
{
  private final Frame frame;
  private final RowSignature signature;
  private final List<FrameColumnReader> columnReaders;

  public FrameCursorFactory(
      final Frame frame,
      final RowSignature signature,
      final List<FrameColumnReader> columnReaders
  )
  {
    this.frame = FrameType.COLUMNAR.ensureType(frame);
    this.signature = signature;
    this.columnReaders = columnReaders;
  }

  @Override
  public boolean canVectorize(@Nullable Filter filter, VirtualColumns virtualColumns, boolean descending)
  {
    // TODO(gianm): impl vectorized cursor enough that it's actually usable
    return false;
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
    final FrameColumnSelector frameColumnSelector =
        new FrameColumnSelector(frame, signature, columnReaders);

    if (Granularities.ALL.equals(gran)) {
      Closer closer = Closer.create();
      final Cursor cursor = makeGranularityAllCursor(
          closer,
          frameColumnSelector,
          filter,
          interval,
          virtualColumns,
          descending
      );

      // Note: if anything closeable is ever added to this Sequence, make sure to update FrameProcessors.makeCursor.
      // Currently, it assumes that closing the Sequence does nothing.
      return Sequences.withBaggage(Sequences.simple(Collections.singletonList(cursor)), closer);
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
        try (final NumericColumn timeColumn = (NumericColumn) timeColumnHolder.getColumn()) {
          firstTime = timeColumn.getLongSingleValueRow(0);
          lastTime = timeColumn.getLongSingleValueRow(frameColumnSelector.getNumRows() - 1);
        }
      }

      final Interval intervalToUse = interval.overlap(Intervals.utc(firstTime, lastTime + 1));

      if (intervalToUse == null) {
        return Sequences.empty();
      }

      return new QueryableIndexCursorSequenceBuilder(
          frameColumnSelector,
          intervalToUse,
          virtualColumns,
          filter,
          queryMetrics,
          firstTime,
          lastTime,
          descending
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

    final FrameColumnSelector columnSelector =
        new FrameColumnSelector(frame, signature, columnReaders);

    final Closer closer = Closer.create();
    final Filter filterToUse = FrameCursorUtils.buildFilter(filter, interval);
    final VectorOffset baseOffset = new NoFilterVectorOffset(vectorSize, 0, frame.numRows());
    final ColumnCache columnCache = new ColumnCache(columnSelector, closer);

    // baseColumnSelectorFactory using baseOffset is the column selector for filtering.
    final VectorColumnSelectorFactory baseColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
        columnSelector,
        baseOffset,
        columnCache,
        virtualColumns
    );

    if (filterToUse == null) {
      return new FrameVectorCursor(frame, baseOffset, baseColumnSelectorFactory, closer);
    } else {
      final FilteredVectorOffset filteredOffset = FilteredVectorOffset.create(
          baseOffset,
          baseColumnSelectorFactory,
          filterToUse
      );

      final VectorColumnSelectorFactory filteredColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
          columnSelector,
          filteredOffset,
          columnCache,
          virtualColumns
      );

      return new FrameVectorCursor(frame, filteredOffset, filteredColumnSelectorFactory, closer);
    }
  }

  private static Cursor makeGranularityAllCursor(
      Closer closer,
      final FrameColumnSelector frameColumnSelector,
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final boolean descending
  )
  {
    final Filter filterToUse = FrameCursorUtils.buildFilter(filter, interval);

    final Offset baseOffset = descending
                              ? new SimpleDescendingOffset(frameColumnSelector.getNumRows())
                              : new SimpleAscendingOffset(frameColumnSelector.getNumRows());

    final Offset offset;

    final QueryableIndexColumnSelectorFactory columnSelectorFactory =
        new QueryableIndexColumnSelectorFactory(
            virtualColumns,
            false,
            baseOffset,
            new ColumnCache(frameColumnSelector, closer)
        );

    if (filterToUse == null) {
      offset = baseOffset;
    } else {
      offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
    }

    return new FrameCursor(offset, columnSelectorFactory);
  }
}
