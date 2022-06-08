/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.row;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.field.FieldReader;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameCursor;
import io.imply.druid.talaria.frame.segment.FrameCursorUtils;
import io.imply.druid.talaria.frame.segment.FrameFilteredOffset;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.data.Offset;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@link CursorFactory} implementation based on a single row-based {@link Frame}.
 *
 * This class is only used for row-based frames. It is not used for columnar frames.
 */
public class FrameCursorFactory implements CursorFactory
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final List<FieldReader> fieldReaders;

  public FrameCursorFactory(
      final Frame frame,
      final FrameReader frameReader,
      final List<FieldReader> fieldReaders
  )
  {
    this.frame = FrameType.ROW_BASED.ensureType(frame);
    this.frameReader = frameReader;
    this.fieldReaders = fieldReaders;
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
    if (Granularities.ALL.equals(gran)) {
      final Cursor cursor = makeGranularityAllCursor(filter, interval, virtualColumns, descending);

      // Note: if anything closeable is ever added to this Sequence, make sure to update FrameProcessors.makeCursor.
      // Currently, it assumes that closing the Sequence does nothing.
      return Sequences.simple(Collections.singletonList(cursor));
    } else {
      // Not currently needed for the intended use cases of row-based frames.
      throw new UOE("Granularity [%s] not supported", gran);
    }
  }

  private Cursor makeGranularityAllCursor(
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final boolean descending
  )
  {
    final Filter filterToUse = FrameCursorUtils.buildFilter(filter, interval);

    final Offset baseOffset = descending
                              ? new SimpleDescendingOffset(frame.numRows())
                              : new SimpleAscendingOffset(frame.numRows());

    final Offset offset;

    final ColumnSelectorFactory columnSelectorFactory =
        virtualColumns.wrap(
            new FrameColumnSelectorFactory(
                frame,
                frameReader.signature(),
                fieldReaders,
                new CursorFrameRowPointer(frame, baseOffset)
            )
        );

    if (filterToUse == null) {
      offset = baseOffset;
    } else {
      offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
    }

    return new FrameCursor(offset, columnSelectorFactory);
  }
}
