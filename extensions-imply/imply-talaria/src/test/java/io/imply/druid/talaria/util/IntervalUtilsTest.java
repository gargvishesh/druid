/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IntervalUtilsTest
{
  @Test
  public void test_difference()
  {
    Assert.assertEquals(
        intervals(),
        IntervalUtils.difference(intervals(), intervals("2000/P1D"))
    );

    Assert.assertEquals(
        intervals("2000/P1D"),
        IntervalUtils.difference(intervals("2000/P1D"), intervals())
    );

    Assert.assertEquals(
        intervals("2000/2001"),
        IntervalUtils.difference(intervals("2000/2001"), intervals("2003/2004"))
    );

    Assert.assertEquals(
        intervals("2000-01-02/2001"),
        IntervalUtils.difference(intervals("2000/2001"), intervals("2000/P1D"))
    );

    Assert.assertEquals(
        intervals("2000/2000-02-01", "2000-02-02/2001"),
        IntervalUtils.difference(intervals("2000/2001"), intervals("2000-02-01/P1D"))
    );

    Assert.assertEquals(
        intervals(),
        IntervalUtils.difference(intervals("2000/2001"), intervals("1999/2001"))
    );

    Assert.assertEquals(
        intervals("2000-01-14/2000-02-01", "2000-02-02/2001"),
        IntervalUtils.difference(intervals("2000/P1D", "2000-01-14/2001"), intervals("2000/P1D", "2000-02-01/P1D"))
    );

    Assert.assertEquals(
        intervals("2000-01-01/2000-07-01", "2000-07-02/2001-01-01", "2002-01-01/2002-07-01", "2002-07-02/2003-01-01"),
        IntervalUtils.difference(intervals("2000/P1Y", "2002/P1Y"), intervals("2000-07-01/P1D", "2002-07-01/P1D"))
    );

    Assert.assertEquals(
        intervals(),
        IntervalUtils.difference(intervals("2000-01-12/2000-01-15"), intervals("2000-01-12/2000-01-13", "2000-01-13/2000-01-16"))
    );

    Assert.assertEquals(
        intervals("2000-07-14/2000-07-15"),
        IntervalUtils.difference(intervals("2000/2001"), intervals("2000-01-01/2000-07-14", "2000-07-15/2001"))
    );
  }

  public static List<Interval> intervals(final String... intervals)
  {
    return Arrays.stream(intervals).map(Intervals::of).collect(Collectors.toList());
  }
}
