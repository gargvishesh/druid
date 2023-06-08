/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.util;

import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.joda.time.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CronFactory
{
  private final ScheduledExecutorService executor;

  public CronFactory(ScheduledExecutorService executor)
  {
    this.executor = executor;
  }

  public Cron make(Callable<ScheduledExecutors.Signal> callable)
  {
    return new Cron(executor, callable);
  }

  public void scheduleOnce(Runnable runnable, Duration duration)
  {
    executor.schedule(runnable, duration.getMillis(), TimeUnit.MILLISECONDS);
  }
}
