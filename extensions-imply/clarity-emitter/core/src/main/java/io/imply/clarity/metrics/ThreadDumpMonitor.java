/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.ArrayList;
import java.util.Map;

public class ThreadDumpMonitor extends AbstractMonitor
{
  private static final String FEED = "threadDump";

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
    String nowString = DateTimes.nowUtc().toString();

    ArrayList<StackTraceEventBuilder> bobs = new ArrayList<>(allStackTraces.size());

    // We first materialize all of the builders before starting to emit anything, this is because the builder itself
    // grabs state from the Thread objects that is suspected to mutate and we want to minimize the race between the
    // state changing on the Thread objects.
    allStackTraces.forEach((thread, stack) -> bobs.add(new StackTraceEventBuilder(nowString, thread, stack)));

    bobs.forEach(emitter::emit);

    return true;
  }

  public static class StackTraceEventBuilder extends ServiceEventBuilder<StackTraceEvent>
  {
    private final String ts;
    private final StackTraceElement[] stack;
    private final String threadName;
    private final long threadId;
    private final Thread.State threadState;

    public StackTraceEventBuilder(
        String ts,
        Thread thread,
        StackTraceElement[] stack
    )
    {
      this.ts = ts;
      this.stack = stack;

      // The Thread object is not a snapshot, but a living object that changes if the thread suddenly wakes up.
      // This means that there is a race for the value of name and thread state.  As such, instead of storing a
      // reference to the Thread, we store a snapshot of what the values were at this point in time.  This doesn't
      // eliminate the race, but it minimizes it to some extent.
      threadName = thread.getName();
      threadId = thread.getId();
      threadState = thread.getState();
    }


    @Override
    public StackTraceEvent build(ImmutableMap<String, String> serviceDimensions)
    {
      return new StackTraceEvent(ts, serviceDimensions, threadName, threadId, threadState, toStackStrings(stack));
    }

    private static String[] toStackStrings(StackTraceElement[] stack)
    {
      String[] retVal = new String[stack.length];
      int i = retVal.length - 1;

      for (StackTraceElement element : stack) {
        retVal[i--] = stackTraceElementToString(element);
      }
      return retVal;
    }

    /**
     * adapted from #StackTraceElement.toString to remove ClassLoader
     */
    private static String stackTraceElementToString(StackTraceElement stackTraceElement)
    {
      String className = stackTraceElement.getClassName();
      boolean isNativeMethod = stackTraceElement.isNativeMethod();
      String methodName = stackTraceElement.getMethodName();
      String fileName = stackTraceElement.getFileName();
      int lineNumber = stackTraceElement.getLineNumber();

      return className + "." + methodName + "(" +
             (isNativeMethod ? "Native Method)" :
              (fileName != null && lineNumber >= 0 ?
               fileName + ":" + lineNumber + ")" :
               (fileName != null ? "" + fileName + ")" : "Unknown Source)")));
    }
  }

  private static class StackTraceEvent implements Event
  {
    private final String ts;
    private final Map<String, String> serviceDimensions;
    private final String threadName;
    private final long threadId;
    private final Thread.State threadState;
    private final String[] stackStrings;

    public StackTraceEvent(
        String ts,
        Map<String, String> serviceDimensions,
        String threadName,
        long threadId,
        Thread.State threadState,
        String[] stackStrings
    )
    {
      this.ts = ts;
      this.serviceDimensions = serviceDimensions;
      this.threadName = threadName;
      this.threadId = threadId;
      this.threadState = threadState;
      this.stackStrings = stackStrings;
    }

    @JsonValue
    @Override
    public EventMap toMap()
    {
      return EventMap.builder()
                     .put("feed", getFeed())
                     .put("timestamp", ts)
                     .putAll(serviceDimensions)
                     .put("threadName", threadName)
                     .put("threadId", threadId)
                     .put("threadState", threadState)
                     .put("stack", stackStrings)
                     .build();
    }

    @Override
    public String getFeed()
    {
      return FEED;
    }
  }
}
