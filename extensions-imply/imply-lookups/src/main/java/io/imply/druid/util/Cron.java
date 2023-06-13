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
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors.Signal;
import org.joda.time.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

public class Cron
{
  private final ScheduledExecutorService executor;
  private final Callable<Signal> callable;

  public Cron(
      ScheduledExecutorService executor,
      Callable<Signal> callable
  )
  {
    this.executor = executor;
    this.callable = callable;
  }

  public void scheduleWithFixedDelay(
      Duration startDelay,
      Duration runDelay
  )
  {
    ScheduledExecutors.scheduleWithFixedDelay(executor, startDelay, runDelay, callable);
  }

  public void submitNow()
  {
    executor.submit(callable);
  }
}
