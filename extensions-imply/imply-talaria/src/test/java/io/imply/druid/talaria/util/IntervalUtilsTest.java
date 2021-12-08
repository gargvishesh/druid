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
    // TODO(gianm): poor coverage; need to test more scenarios

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
  }

  public static List<Interval> intervals(final String... intervals)
  {
    return Arrays.stream(intervals).map(Intervals::of).collect(Collectors.toList());
  }
}
