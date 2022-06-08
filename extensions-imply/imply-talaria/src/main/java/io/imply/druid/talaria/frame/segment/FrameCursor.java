/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.data.Offset;
import org.joda.time.DateTime;

/**
 * A simple {@link Cursor} that increments an offset.
 */
public class FrameCursor implements Cursor
{
  private final Offset offset;
  private final ColumnSelectorFactory columnSelectorFactory;

  public FrameCursor(Offset offset, ColumnSelectorFactory columnSelectorFactory)
  {
    this.offset = offset;
    this.columnSelectorFactory = columnSelectorFactory;
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public DateTime getTime()
  {
    // TODO(gianm): What's a sensible cursor time? Get it from the frame, maybe?
    return DateTimes.utc(0);
  }

  @Override
  public void advance()
  {
    offset.increment();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    offset.increment();
  }

  @Override
  public boolean isDone()
  {
    return !offset.withinBounds();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    offset.reset();
  }
}
