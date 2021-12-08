/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import io.imply.druid.talaria.frame.boost.SettableLongVirtualColumn;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.joda.time.DateTime;

/**
 * Used by {@link FrameTestUtil#readRowsFromAdapter} and {@link FrameTestUtil#readRowsFromCursor}.
 */
public class RowNumberUpdatingCursor implements Cursor
{
  private final Cursor baseCursor;
  private final SettableLongVirtualColumn rowNumberVirtualColumn;

  RowNumberUpdatingCursor(Cursor baseCursor, SettableLongVirtualColumn rowNumberVirtualColumn)
  {
    this.baseCursor = baseCursor;
    this.rowNumberVirtualColumn = rowNumberVirtualColumn;
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return baseCursor.getColumnSelectorFactory();
  }

  @Override
  public DateTime getTime()
  {
    return baseCursor.getTime();
  }

  @Override
  public void advance()
  {
    rowNumberVirtualColumn.setValue(rowNumberVirtualColumn.getValue() + 1);
    baseCursor.advance();
  }

  @Override
  public void advanceUninterruptibly()
  {
    rowNumberVirtualColumn.setValue(rowNumberVirtualColumn.getValue() + 1);
    baseCursor.advanceUninterruptibly();
  }

  @Override
  public boolean isDone()
  {
    return baseCursor.isDone();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return baseCursor.isDoneOrInterrupted();
  }

  @Override
  public void reset()
  {
    rowNumberVirtualColumn.setValue(0);
    baseCursor.reset();
  }
}
