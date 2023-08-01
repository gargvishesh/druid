/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.license;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.guice.LazySingleton;

public class ImplyLicenseModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(ImplyLicenseManager.class).toProvider(ImplyLicenseManagerProvider.class).in(LazySingleton.class);
  }
}