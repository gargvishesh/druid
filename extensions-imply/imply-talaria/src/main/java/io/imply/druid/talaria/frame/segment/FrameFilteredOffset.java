/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment;

import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;

/**
 * Copy of {@link org.apache.druid.segment.FilteredOffset} that does not require bitmap indexes.
 *
 * In a future where {@link org.apache.druid.segment.FilteredOffset} is opened up for usage outside of regular segments,
 * this class could be removed and usages could be migrated to {@link org.apache.druid.segment.FilteredOffset}.
 */
public class FrameFilteredOffset extends Offset
{
  private final Offset baseOffset;
  private final ValueMatcher filterMatcher;

  public FrameFilteredOffset(
      final Offset baseOffset,
      final ColumnSelectorFactory columnSelectorFactory,
      final Filter postFilter
  )
  {
    this.baseOffset = baseOffset;
    this.filterMatcher = postFilter.makeMatcher(columnSelectorFactory);
    incrementIfNeededOnCreationOrReset();
  }

  @Override
  public void increment()
  {
    while (!Thread.currentThread().isInterrupted()) {
      baseOffset.increment();
      if (!baseOffset.withinBounds() || filterMatcher.matches()) {
        return;
      }
    }
  }

  @Override
  public boolean withinBounds()
  {
    return baseOffset.withinBounds();
  }

  @Override
  public void reset()
  {
    baseOffset.reset();
    incrementIfNeededOnCreationOrReset();
  }

  private void incrementIfNeededOnCreationOrReset()
  {
    if (baseOffset.withinBounds()) {
      if (!filterMatcher.matches()) {
        increment();
        // increment() returns early if it detects the current Thread is interrupted. It will leave this
        // FilteredOffset in an illegal state, because it may point to an offset that should be filtered. So must
        // call BaseQuery.checkInterrupted() and thereby throw a QueryInterruptedException.
        BaseQuery.checkInterrupted();
      }
    }
  }

  @Override
  public ReadableOffset getBaseReadableOffset()
  {
    return baseOffset.getBaseReadableOffset();
  }

  /**
   * See {@link org.apache.druid.segment.FilteredOffset#clone()} for notes.
   */
  @Override
  public Offset clone()
  {
    throw new UnsupportedOperationException("FrameFilteredOffset cannot be cloned");
  }

  @Override
  public int getOffset()
  {
    return baseOffset.getOffset();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("baseOffset", baseOffset);
    inspector.visit("filterMatcher", filterMatcher);
  }
}
