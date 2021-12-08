/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

/**
 * Things that would make sense in {@link org.apache.druid.java.util.common.Intervals} if this were not an extension.
 */
public class IntervalUtils
{
  public static List<Interval> difference(final List<Interval> list1, final List<Interval> list2)
  {
    final List<Interval> retVal = new ArrayList<>();

    int i = 0, j = 0;
    while (i < list1.size()) {
      while (j < list2.size() && list2.get(j).isBefore(list1.get(i))) {
        j++;
      }

      if (j == list2.size() || list2.get(j).isAfter(list1.get(i))) {
        retVal.add(list1.get(i));
        i++;
      } else {
        final Interval overlap = list1.get(i).overlap(list2.get(j));
        final Interval a = new Interval(list1.get(i).getStart(), overlap.getStart());
        final Interval b = new Interval(overlap.getEnd(), list1.get(i).getEnd());

        if (a.toDurationMillis() > 0) {
          retVal.add(a);
        }

        if (b.toDurationMillis() > 0) {
          list1.set(i, b);
        } else {
          i++;
        }
      }
    }

    return retVal;
  }
}
