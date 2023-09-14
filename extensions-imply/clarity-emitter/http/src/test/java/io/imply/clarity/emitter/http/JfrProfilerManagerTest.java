/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class JfrProfilerManagerTest
{
  @Test
  public void testCreation() throws Exception
  {
    Properties properties = System.getProperties();

    testJfrProfilerManagerCreateAndRun(properties);
  }

  @Test
  public void testJdk8()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException
  {
    // if we are running in jdk17+, this test forces testing the case that the JfrProfilerManager still appears
    // to function
    Properties properties = System.getProperties();

    properties.setProperty("java.version", "1.8.0_212");

    testJfrProfilerManagerCreateAndRun(properties);
  }

  private static void testJfrProfilerManagerCreateAndRun(Properties properties)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    ClarityHttpEmitterConfig clarityHttpEmitterConfig =
        ClarityHttpEmitterConfig.builder("http://metrics.foo.bar/")
                                .withBasicAuthentication("user1:pass1")
                                .withTimeOut(new Period("PT10S"))
                                .withFlushMillis(1000)
                                .build();
    JfrProfilerManagerConfig jfrProfilerManagerConfig =
        JfrProfilerManagerConfig.builder()
                                .addTag("key1", "value1")
                                .addTag("key2", "value2")
                                .build();
    JdkVersion jdkVersion = JdkVersion.create(properties);
    JfrProfilerManager jfrProfilerManager = new JfrProfilerManager(
        clarityHttpEmitterConfig,
        jfrProfilerManagerConfig,
        jdkVersion
    );

    Assert.assertNotNull(jfrProfilerManager);

    try {
      jfrProfilerManager.start();
    }
    finally {
      jfrProfilerManager.stop();
    }
  }
}
