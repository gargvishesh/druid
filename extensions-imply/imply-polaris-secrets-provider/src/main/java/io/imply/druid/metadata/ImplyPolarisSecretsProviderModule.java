/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.metadata;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class ImplyPolarisSecretsProviderModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("ImplyPolarisSecretsProviderModule").registerSubtypes(
            new NamedType(ImplyPolarisSecretsConfluentConfigOverrides.class, "polarisSecretsConfluent")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "imply.polaris.secrets", ImplyPolarisSecretsProviderConfig.class);
  }
}

