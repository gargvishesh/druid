/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;

/**
 * uses class reflection to load the jfr profiler as it requires jdk17+. This avoids any imports of the jfr profiler
 * classes which are compiled with a target of jdk17+. This class is only loaded if the jfr profiler is enabled and
 * we are on jdk17+
 */
public class JfrProfilerManager
{
  private static final Logger log = new Logger(JfrProfilerManager.class);

  private static final int MIN_JDK_VERSION = 17;

  private final Object profiler;
  private final Method profilerStart;
  private final Method profilerStop;

  @Inject
  public JfrProfilerManager(
      ClarityHttpEmitterConfig clarityHttpEmitterConfig,
      JfrProfilerManagerConfig jfrProfilerManagerConfig,
      JdkVersion jdkVersion
  ) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    if (jdkVersion.getMajorVersion() < MIN_JDK_VERSION) {
      profiler = null;
      profilerStart = null;
      profilerStop = null;
    } else {
      String basicAuthentication = clarityHttpEmitterConfig.getBasicAuthentication();
      String[] parts = basicAuthentication.split(":");
      long readTimeoutMillis = clarityHttpEmitterConfig.getReadTimeout().toStandardDuration().getMillis();
      String recipientBaseUrl = clarityHttpEmitterConfig.getRecipientBaseUrl();
      // the jfr profiler URL is of the form "https://<host>/soln/profiling/<user>/events". Build from the clarity
      // url's host and the user from the http basic auth
      URI baseUri = URI.create(recipientBaseUrl);
      String host = baseUri.getHost();
      URI jfrProfilerUri = URI.create(StringUtils.format("https://%s/soln/profiling/%s/events", host, parts[0]));

      Preconditions.checkArgument(
          readTimeoutMillis <= Integer.MAX_VALUE,
          "readTimeoutMillis must be less than or equal to %s", Integer.MAX_VALUE
      );

      // we have to access this class via reflection since we are in a codebase that might run on jdk8 or jdk11. the
      // jfr-profiler is compiled with target jdk17. this avoids any static references to the jdk17 classes in
      // jfr-profiler.
      Class<?> profilerFactoryClass = Class.forName("io.imply.jfr_profiler.creation.ProfilerFactory");
      Method createProfilerMethod = profilerFactoryClass.getMethod("createProfiler", Map.class);
      Map<String, Object> params =
          ImmutableMap.<String, Object>builder()
                      .put("username", parts[0])
                      .put("password", parts[1])
                      .put("url", jfrProfilerUri)
                      .put("httpRequestTimeoutMillis", (int) readTimeoutMillis)
                      .put("eventAggregatorFlushIntervalMillis", clarityHttpEmitterConfig.getFlushMillis())
                      .put("tags", jfrProfilerManagerConfig.getTags())
                      .build();
      profiler = createProfilerMethod.invoke(null, params);
      profilerStart = profiler.getClass().getMethod("start");
      profilerStop = profiler.getClass().getMethod("close");
      log.info("jfr profiler created");
    }
  }

  @LifecycleStart
  public void start() throws InvocationTargetException, IllegalAccessException
  {
    if (profiler != null) {
      log.info("jfr profiler started");
      profilerStart.invoke(profiler);
    }
  }

  @LifecycleStop
  public void stop() throws InvocationTargetException, IllegalAccessException
  {
    if (profiler != null) {
      log.info("jfr profiler stopped");
      profilerStop.invoke(profiler);
    }
  }
}
