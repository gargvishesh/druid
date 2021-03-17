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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.imply.druid.security.keycloak.authorization.db.updater.CoordinatorKeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.db.updater.NoopKeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.endpoint.CoordinatorKeycloakAuthorizerResourceHandler;
import io.imply.druid.security.keycloak.authorization.endpoint.DefaultKeycloakAuthorizerResourceHandler;
import io.imply.druid.security.keycloak.authorization.endpoint.KeycloakAuthorizerResourceHandler;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
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

    LifecycleModule.register(binder, KeycloakAuthorizerMetadataStorageUpdater.class);
  }

  @Provides
  @LazySingleton
  public static KeycloakAuthorizerResourceHandler createAuthorizerResourceHandler(
      final Injector injector
  ) throws ClassNotFoundException
  {
    return getInstance(
        injector,
        CoordinatorKeycloakAuthorizerResourceHandler.class,
        DefaultKeycloakAuthorizerResourceHandler.class
    );
  }

  @Provides
  @LazySingleton
  public static KeycloakAuthorizerMetadataStorageUpdater createAuthorizerStorageUpdater(
      final Injector injector
  ) throws ClassNotFoundException
  {
    return getInstance(
        injector,
        CoordinatorKeycloakAuthorizerMetadataStorageUpdater.class,
        NoopKeycloakAuthorizerMetadataStorageUpdater.class
    );
  }

  /**
   * Returns the instance provided either by a config property or coordinator-run class or default class.
   * The order of check corresponds to the order of method params.
   */
  private static <T> T getInstance(
      Injector injector,
      Class<? extends T> classRunByCoordinator,
      Class<? extends T> defaultClass
  ) throws ClassNotFoundException
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(classRunByCoordinator);
    }
    return injector.getInstance(defaultClass);
  }

  private static boolean isCoordinator(Injector injector)
  {
    final String serviceName;
    try {
      serviceName = injector.getInstance(Key.get(String.class, Names.named("serviceName")));
    }
    catch (Exception e) {
      return false;
    }

    return "druid/coordinator".equals(serviceName);
  }
}
