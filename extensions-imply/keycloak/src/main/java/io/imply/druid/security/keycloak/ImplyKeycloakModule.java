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
import com.google.inject.TypeLiteral;
import io.imply.druid.security.keycloak.authorization.db.updater.CoordinatorKeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.db.updater.NoopKeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.endpoint.CoordinatorKeycloakAuthorizerResourceHandler;
import io.imply.druid.security.keycloak.authorization.endpoint.DefaultKeycloakAuthorizerResourceHandler;
import io.imply.druid.security.keycloak.authorization.endpoint.KeycloakAuthorizerResource;
import io.imply.druid.security.keycloak.authorization.endpoint.KeycloakAuthorizerResourceHandler;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.keycloak.representations.adapters.config.AdapterConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public class ImplyKeycloakModule implements DruidModule
{
  private static final Logger LOG = new Logger(ImplyKeycloakModule.class);

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

    Jerseys.addResource(binder, KeycloakAuthorizerResource.class);

    LifecycleModule.register(binder, KeycloakAuthorizerMetadataStorageUpdater.class);
  }

  @Provides
  @LazySingleton
  public static KeycloakAuthorizerResourceHandler createAuthorizerResourceHandler(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getResourceHandlerClassForService(nodeRoles));
  }

  @Provides
  @LazySingleton
  public static KeycloakAuthorizerMetadataStorageUpdater createAuthorizerStorageUpdater(
      final Injector injector
  )
  {
    // TODO: Coordinator storage updater is bound for all services now since there is no caching layer at the moment
    //       This will be fixed with IMPLY-6252
    return injector.getInstance(CoordinatorKeycloakAuthorizerMetadataStorageUpdater.class);
  }

  private static Class<? extends KeycloakAuthorizerResourceHandler> getResourceHandlerClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorKeycloakAuthorizerResourceHandler.class;
    } else {
      return DefaultKeycloakAuthorizerResourceHandler.class;
    }
  }

  private static Class<? extends KeycloakAuthorizerMetadataStorageUpdater> getStorageUpdaterClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorKeycloakAuthorizerMetadataStorageUpdater.class;
    } else {
      return NoopKeycloakAuthorizerMetadataStorageUpdater.class;
    }
  }

  @Nullable
  public static Set<NodeRole> getNodeRoles(Injector injector)
  {
    try {
      return injector.getInstance(
          Key.get(
              new TypeLiteral<Set<NodeRole>>()
              {
              },
              Self.class
          )
      );
    }
    catch (Exception e) {
      LOG.error(e, "Got exception while getting node roles.");
      return null;
    }
  }

  public static boolean isCoordinatorRole(Set<NodeRole> nodeRoles)
  {
    if (nodeRoles == null) {
      return false;
    }
    return nodeRoles.contains(NodeRole.COORDINATOR);
  }
}
