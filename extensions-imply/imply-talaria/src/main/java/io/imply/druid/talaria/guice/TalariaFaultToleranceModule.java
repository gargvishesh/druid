/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import io.imply.druid.storage.StorageConnector;
import io.imply.druid.storage.StorageConnectorProvider;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.Properties;


public class TalariaFaultToleranceModule implements DruidModule
{
  private static final Logger log = new Logger(TalariaFaultToleranceModule.class);
  @Inject
  private Properties properties;


  public TalariaFaultToleranceModule()
  {
  }

  public TalariaFaultToleranceModule(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    if (isFaultToleranceEnabled()) {
      JsonConfigProvider.bind(
          binder,
          TalariaIndexingModule.TALARIA_INTERMEDIATE_STORAGE,
          StorageConnectorProvider.class,
          Talaria.class
      );
      binder.bind(Key.get(StorageConnector.class, Talaria.class))
            .toProvider(Key.get(StorageConnectorProvider.class, Talaria.class))
            .in(LazySingleton.class);
    }
  }
  private boolean isFaultToleranceEnabled()
  {
    return Boolean.parseBoolean((String) properties.getOrDefault(
        TalariaIndexingModule.TALARIA_INTERMEDIATE_STORAGE_ENABLED,
        "false"
    ));
  }
}
