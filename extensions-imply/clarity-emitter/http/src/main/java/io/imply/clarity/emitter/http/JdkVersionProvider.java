/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.google.inject.Inject;
import com.google.inject.Provider;

import java.util.Properties;

public class JdkVersionProvider implements Provider<JdkVersion>
{
  private final Properties properties;

  @Inject
  public JdkVersionProvider(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public JdkVersion get()
  {
    return JdkVersion.create(properties);
  }
}
