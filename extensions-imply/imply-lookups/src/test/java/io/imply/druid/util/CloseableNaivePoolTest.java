/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class CloseableNaivePoolTest
{
  private ArrayList<CloseableThing> generatedThings;
  private CloseableNaivePool<CloseableThing> pool;

  @Before
  public void setUp()
  {
    generatedThings = new ArrayList<>();
    pool = new CloseableNaivePool<>("poolOfString", () -> {
      final CloseableThing retVal = new CloseableThing();
      generatedThings.add(retVal);
      return retVal;
    });
  }

  @After
  public void tearDown()
  {
    pool.close();
    for (CloseableThing generatedThing : generatedThings) {
      Assert.assertEquals(1, generatedThing.closedTimes.get());
    }
  }

  @Test
  public void testTakeAndClose()
  {
    final CloseableNaivePool.PooledResourceHolder<CloseableThing> taken = pool.take();
    Assert.assertEquals(1, generatedThings.size());
    Assert.assertSame(generatedThings.get(0), taken.get());
    taken.close();
    Assert.assertEquals(0, generatedThings.get(0).closedTimes.get());
  }

  private static class CloseableThing implements Closeable
  {
    private AtomicInteger closedTimes = new AtomicInteger(0);

    @Override
    public void close()
    {
      closedTimes.incrementAndGet();
    }
  }
}
