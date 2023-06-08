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
