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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.HashMap;
import java.util.Map;

public class ThreadDumpMonitor extends AbstractMonitor
{
  private static final String FEED = "threadDump";

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
    String nowString = DateTimes.nowUtc().toString();

    allStackTraces.forEach((thread, stackTraceElements) -> {
      Map<String, Object> baseEventMap = new HashMap<>();

      baseEventMap.put("timestamp", nowString);
      baseEventMap.put("threadName", thread.getName());
      baseEventMap.put("threadId", thread.getId());
      baseEventMap.put("threadState", thread.getState());

      String[] asStringsReversed = new String[stackTraceElements.length];
      int i = asStringsReversed.length - 1;

      for (StackTraceElement element : stackTraceElements) {
        asStringsReversed[i--] = stackTraceElementToString(element);
      }

      baseEventMap.put("stack", asStringsReversed);

      ServiceEventBuilder<StackTraceEvent> serviceEventBuilder = new ServiceEventBuilder<StackTraceEvent>()
      {
        @Override
        public StackTraceEvent build(ImmutableMap<String, String> serviceDimensions)
        {
          return new StackTraceEvent(baseEventMap, serviceDimensions);
        }
      };

      emitter.emit(serviceEventBuilder);
    });

    return true;
  }

  /**
   * adapted from #StackTraceElement.toString to remove ClassLoader
   */
  private static String stackTraceElementToString(StackTraceElement stackTraceElement)
  {
    String className = stackTraceElement.getClassName();
    String s = className;
    boolean isNativeMethod = stackTraceElement.isNativeMethod();
    String methodName = stackTraceElement.getMethodName();
    String fileName = stackTraceElement.getFileName();
    int lineNumber = stackTraceElement.getLineNumber();

    return s + "." + methodName + "(" +
           (isNativeMethod ? "Native Method)" :
            (fileName != null && lineNumber >= 0 ?
             fileName + ":" + lineNumber + ")" :
             (fileName != null ? "" + fileName + ")" : "Unknown Source)")));
  }

  private static class StackTraceEvent implements Event
  {
    private final Supplier<EventMap> eventMapSupplier;

    public StackTraceEvent(Map<String, Object> baseEventMap, Map<String, String> serviceDimensions)
    {
      eventMapSupplier = Suppliers.memoize(() -> EventMap.builder()
                                                         .putAll(baseEventMap)
                                                         .putAll(serviceDimensions)
                                                         .build());
    }

    @JsonValue
    @Override
    public EventMap toMap()
    {
      return eventMapSupplier.get();
    }

    @Override
    public String getFeed()
    {
      return FEED;
    }
  }
}
