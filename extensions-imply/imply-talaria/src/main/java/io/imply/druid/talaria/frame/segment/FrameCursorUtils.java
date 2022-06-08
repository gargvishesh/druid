/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;

public class FrameCursorUtils
{
  private FrameCursorUtils()
  {
    // No instantiation.
  }

  /**
   * Builds a {@link Filter} from a {@link Filter} plus an {@link Interval}. Useful when we want to do a time filter
   * on a frame, but can't push the time filter into the frame itself (perhaps because it isn't time-sorted).
   */
  @Nullable
  public static Filter buildFilter(@Nullable Filter filter, Interval interval)
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
}
