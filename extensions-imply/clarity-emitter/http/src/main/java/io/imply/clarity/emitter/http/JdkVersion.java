/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import java.util.Properties;

public class JdkVersion
{
  private final int majorVersion;

  public JdkVersion(int majorVersion)
  {
    this.majorVersion = majorVersion;
  }

  public static JdkVersion create(Properties properties)
  {
    return new JdkVersion(getMajorJavaVersion(properties));
  }

  public int getMajorVersion()
  {
    return majorVersion;
  }

  private static int getMajorJavaVersion(Properties properties)
  {
    String version = properties.getProperty("java.version");
    String[] parts = version.split("\\.");

    if (parts[0].equals("1")) {
      return Integer.parseInt(parts[1]);
    } else {
      return Integer.parseInt(parts[0]);
    }
  }
}
