/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class JdkVersionTest
{
  @Test
  public void testJdk8()
  {
    testJdkVersion("1.8.0_212", 8);
  }

  @Test
  public void testJdk11()
  {
    testJdkVersion("11.0.2_884", 11);
  }

  @Test
  public void testJdk17()
  {
    testJdkVersion("17.0.2", 17);
  }

  private static void testJdkVersion(String javaVersion, int expectedMajorVersion)
  {
    Properties properties = new Properties();

    properties.setProperty("java.version", javaVersion);

    JdkVersion jdkVersion = JdkVersion.create(properties);

    Assert.assertEquals(expectedMajorVersion, jdkVersion.getMajorVersion());
  }
}
