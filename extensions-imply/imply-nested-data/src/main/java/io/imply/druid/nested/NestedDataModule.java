/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Collections;
import java.util.List;

public class NestedDataModule implements DruidModule
{
  private static final Logger LOG = new Logger(NestedDataModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    // do nothing, this stuff is baked into core Druid now
    LOG.info("The functionality of imply-nested-data is now part of Apache Druid. The imply-nested-data extension is now an empty placeholder and can safely be removed from your load list");
  }
}
