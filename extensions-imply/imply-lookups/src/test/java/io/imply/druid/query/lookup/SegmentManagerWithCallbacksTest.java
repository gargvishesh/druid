/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class SegmentManagerWithCallbacksTest
{
  @Test
  public void testSanity() throws SegmentLoadingException
  {
    SegmentManagerWithCallbacks manager = new SegmentManagerWithCallbacks(new SegmentFilterLookupTestsLoader());

    AtomicInteger callCounter = new AtomicInteger(0);

    final Runnable runnable = callCounter::incrementAndGet;
    final Runnable runnable2 = callCounter::incrementAndGet;

    Assert.assertNotSame(runnable, runnable2);

    manager.registerCallback("withCallback", runnable);
    manager.registerCallback("withCallback", runnable2);

    Assert.assertEquals(0, callCounter.get());
    manager.loadSegment(
        DataSegment.builder().dataSource("filterable").interval(Intervals.ETERNITY).size(1).version("123").build(),
        false,
        null
    );
    Assert.assertEquals(0, callCounter.get());


    final DataSegment.Builder withCallbackBob =
        DataSegment.builder().dataSource("withCallback").interval(Intervals.ETERNITY).size(1);

    Assert.assertTrue(manager.loadSegment(withCallbackBob.version("123").build(), false, null));
    Assert.assertEquals(2, callCounter.get());
    Assert.assertFalse(manager.loadSegment(withCallbackBob.version("123").build(), false, null));
    Assert.assertEquals(2, callCounter.get());

    manager.unregisterCallback("withCallback", runnable2);
    Assert.assertFalse(manager.loadSegment(withCallbackBob.version("123").build(), false, null));
    Assert.assertEquals(2, callCounter.get());
    Assert.assertTrue(manager.loadSegment(withCallbackBob.version("124").build(), false, null));
    Assert.assertEquals(3, callCounter.get());
    Assert.assertTrue(manager.loadSegment(withCallbackBob.version("125").build(), false, null));
    Assert.assertEquals(4, callCounter.get());

    manager.unregisterCallback("withCallback", runnable);
    Assert.assertTrue(manager.loadSegment(withCallbackBob.version("126").build(), false, null));
    Assert.assertEquals(4, callCounter.get());
    Assert.assertTrue(manager.loadSegment(withCallbackBob.version("127").build(), false, null));
    Assert.assertEquals(4, callCounter.get());
  }
}
