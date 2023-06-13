/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.util;

import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("ALL")
public class CronFactoryTest
{
  @SuppressWarnings({"ConstantConditions", "ReturnOfNull"})
  @Test
  public void testMake()
  {
    AtomicLong expectedInitialDelay = new AtomicLong(0);
    AtomicLong expectedDelay = new AtomicLong(0);

    CronFactory factory = new CronFactory(
        new TestScheduledExecutorService()
        {
          int callNum = 0;

          @Override
          public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
          {
            ++callNum;
            if (callNum == 1) {
              Assert.assertEquals(expectedInitialDelay.get(), delay);
              Assert.assertEquals(TimeUnit.MILLISECONDS, unit);
              command.run();
            } else if (callNum == 2) {
              Assert.assertEquals(expectedDelay.get(), delay);
              Assert.assertEquals(TimeUnit.MILLISECONDS, unit);
              command.run();
            }
            return null;
          }

          @Override
          public <T> Future<T> submit(Callable<T> task)
          {
            try {
              task.call();
            }
            catch (Exception e) {
              throw new RE(e);
            }
            return null;
          }
        }
    );

    AtomicInteger timesCalled = new AtomicInteger(0);
    final Cron cron = factory.make(() -> {
      final int callCount = timesCalled.incrementAndGet();
      return callCount == 0 ? ScheduledExecutors.Signal.REPEAT : ScheduledExecutors.Signal.STOP;
    });

    Assert.assertEquals(0, timesCalled.get());
    expectedInitialDelay.set(5000);
    expectedDelay.set(15000);
    cron.scheduleWithFixedDelay(Duration.standardSeconds(5), Duration.standardSeconds(15));
    Assert.assertEquals(1, timesCalled.get());

    cron.submitNow();
    Assert.assertEquals(2, timesCalled.get());
  }
}
