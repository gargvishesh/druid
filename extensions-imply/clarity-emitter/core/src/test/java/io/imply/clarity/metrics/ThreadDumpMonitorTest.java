/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ThreadDumpMonitorTest
{
  private final ThreadDumpMonitor threadDumpMonitor = new ThreadDumpMonitor();
  private final List<Event> resultEvents = new ArrayList<>();

  private ServiceEmitter serviceEmitter;

  @Before
  public void setUp()
  {
    NoopEmitter eventCapturingEmitter = new NoopEmitter()
    {
      @Override
      public void emit(Event event)
      {
        resultEvents.add(event);
      }
    };

    serviceEmitter = new ServiceEmitter("fuu", "bar", eventCapturingEmitter);
  }

  @Test
  public void testEventFields() throws Exception
  {
    int numThreads = 1;

    runWithThreads("thread-lightly", numThreads, () -> threadDumpMonitor.doMonitor(serviceEmitter));
    Assert.assertTrue(resultEvents.size() >= numThreads);

    List<Event> eventList =
        resultEvents.stream()
                    .filter(e -> ((String) (e.toMap().get("threadName"))).startsWith("thread-lightly-"))
                    .collect(Collectors.toList());

    Assert.assertEquals(numThreads, eventList.size());

    for (Event event : eventList) {
      EventMap eventMap = event.toMap();

      Assert.assertEquals("threadDump", eventMap.get("feed"));
      Assert.assertTrue(eventMap.get("timestamp") instanceof String);
      Assert.assertEquals("fuu", eventMap.get("service"));
      Assert.assertEquals("bar", eventMap.get("host"));
      Assert.assertTrue(eventMap.get("threadId") instanceof Long);
      Assert.assertTrue(eventMap.get("threadState") instanceof Thread.State);
      Assert.assertTrue(eventMap.get("stack") instanceof String[]);

      String[] stack = (String[]) eventMap.get("stack");
      Assert.assertTrue(stack[1].startsWith(getClass().getName() + "$ThreadBlocked.run(ThreadDumpMonitorTest.java:"));
    }
  }

  @Test
  public void testLambda()
  {

  }

  private static void runWithThreads(String namePrefix, int numThreads, Runnable runnable) throws Exception
  {
    List<Thread> threadList = new ArrayList<>();
    List<ThreadBlocked> threadBlockedList = new ArrayList<>();
    CountDownLatch startLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      ThreadBlocked threadBlocked = new ThreadBlocked(startLatch);
      Thread thread = new Thread(threadBlocked, namePrefix + "-" + i);

      thread.start();
      threadList.add(thread);
      threadBlockedList.add(threadBlocked);
    }

    // make sure all threads reach the run method
    startLatch.await();

    try {
      runnable.run();
    }
    finally {
      Exception exception = null;
      int i = 0;

      for (Thread thread : threadList) {
        ThreadBlocked threadBlocked = threadBlockedList.get(i++);

        try {
          threadBlocked.completionLatch.countDown();
          thread.join();
        }
        catch (Exception e) {
          exception = e;
        }
      }

      if (exception != null) {
        throw exception;
      }
    }
  }

  private static class ThreadBlocked implements Runnable
  {
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final CountDownLatch startLatch;

    public ThreadBlocked(CountDownLatch startLatch)
    {
      this.startLatch = startLatch;
    }

    @Override
    public void run()
    {
      try {
        startLatch.countDown();
        completionLatch.await();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
