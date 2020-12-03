/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.initialization.DruidModule;
import org.keycloak.representations.adapters.config.AdapterConfig;

import java.util.List;

public class ImplyKeycloakModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(ImplyKeycloakModule.class.getSimpleName())
            .registerSubtypes(
                ImplyKeycloakAuthenticator.class,
                ImplyKeycloakAuthorizer.class,
                ImplyKeycloakEscalator.class
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.keycloak", AdapterConfig.class);
    JsonConfigProvider.bind(binder, "druid.escalator.keycloak", AdapterConfig.class, EscalatedGlobal.class);
  }
}
