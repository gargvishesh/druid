/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.server.DruidNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Properties;

public class ClarityHttpEmitterModuleTest
{
  private Properties properties;

  @Before
  public void setUp()
  {
    properties = System.getProperties();
    properties.setProperty(ClarityHttpEmitterModule.PROPERTY_JFR_PROFILER, "true");
  }

  @After
  public void tearDown()
  {
    properties.remove(ClarityHttpEmitterModule.PROPERTY_JFR_PROFILER);
  }

  @Test
  public void testDefaultWorkerCount()
  {
    HttpClientConfig clientConfig = ClarityHttpEmitterModule.getHttpClientConfig(
        ClarityHttpEmitterConfig.builder("http://metrics.foo.bar/").build(),
        new ClarityHttpEmitterSSLClientConfig()
    );
    Assert.assertEquals(ClarityHttpEmitterModule.getDefaultWorkerCount(), clientConfig.getWorkerPoolSize());
  }

  @Test
  public void testWorkerCount()
  {
    final int workerCount = 12;
    HttpClientConfig clientConfig = ClarityHttpEmitterModule.getHttpClientConfig(
        ClarityHttpEmitterConfig.builder("http://metrics.foo.bar/").withWorkerCount(workerCount).build(),
        new ClarityHttpEmitterSSLClientConfig()
    );
    Assert.assertEquals(workerCount, clientConfig.getWorkerPoolSize());
  }

  @Test
  public void testGetJfrProfilerManagerConfig()
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
             IllegalAccessException, NoSuchFieldException
  {
    ClarityHttpEmitterModule module = new ClarityHttpEmitterModule();

    module.setProperties(properties);

    ClarityHttpEmitterConfig clarityHttpEmitterConfig = getClarityHttpEmitterConfig();

    Properties jfrProfilerManageConfigProperties = new Properties();

    jfrProfilerManageConfigProperties.setProperty("druid.metrics.emitter.dimension.taskId", "10001");

    JfrProfilerManagerConfig jfrProfilerManagerConfig = module.getJfrProfilerManagerConfig(
        clarityHttpEmitterConfig,
        new DruidNode("serviceName", "host", true, 8080, 8081, 8082, false, true),
        jfrProfilerManageConfigProperties
    );

    Assert.assertNotNull(jfrProfilerManagerConfig);
  }

  private static ClarityHttpEmitterConfig getClarityHttpEmitterConfig()
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
             InvocationTargetException, NoSuchFieldException
  {
    Class<?> clazz = Class.forName("io.imply.clarity.emitter.http.ClarityHttpEmitterConfig");
    Constructor<?> constructor = clazz.getDeclaredConstructor();

    constructor.setAccessible(true);

    ClarityHttpEmitterConfig clarityHttpEmitterConfig = (ClarityHttpEmitterConfig) constructor.newInstance();
    Field field = clazz.getDeclaredField("jfrProfilerTags");

    field.setAccessible(true);
    field.set(clarityHttpEmitterConfig, Collections.emptyMap());

    return clarityHttpEmitterConfig;
  }
}
