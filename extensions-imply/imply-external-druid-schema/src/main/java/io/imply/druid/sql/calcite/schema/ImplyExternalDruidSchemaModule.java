/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.imply.druid.sql.calcite.schema.tables.endpoint.BrokerExternalDruidSchemaResourceHandler;
import io.imply.druid.sql.calcite.schema.tables.endpoint.CoordinatorExternalDruidSchemaResourceHandler;
import io.imply.druid.sql.calcite.schema.tables.endpoint.DefaultExternalDruidSchemaResourceHandler;
import io.imply.druid.sql.calcite.schema.tables.endpoint.ExternalDruidSchemaResource;
import io.imply.druid.sql.calcite.schema.tables.endpoint.ExternalDruidSchemaResourceHandler;
import io.imply.druid.sql.calcite.schema.tables.state.cache.CoordinatorExternalDruidSchemaCacheManager;
import io.imply.druid.sql.calcite.schema.tables.state.cache.CoordinatorPollingExternalDruidSchemaCacheManager;
import io.imply.druid.sql.calcite.schema.tables.state.cache.DefaultExternalDruidSchemaCacheManager;
import io.imply.druid.sql.calcite.schema.tables.state.cache.ExternalDruidSchemaCacheManager;
import io.imply.druid.sql.calcite.schema.tables.state.notifier.CoordinatorExternalDruidSchemaCacheNotifier;
import io.imply.druid.sql.calcite.schema.tables.state.notifier.ExternalDruidSchemaCacheNotifier;
import io.imply.druid.sql.calcite.schema.tables.state.notifier.NoopExternalDruidSchemaCacheNotifier;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ImplyExternalDruidSchemaModule implements DruidModule
{
  public static final String TYPE = "imply";

  private static final Logger log = new Logger(ImplyExternalDruidSchemaModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.sql.schemamanager.imply", ImplyExternalDruidSchemaCommonCacheConfig.class);

    PolyBind.optionBinder(binder, Key.get(DruidSchemaManager.class))
            .addBinding(TYPE)
            .to(ImplyDruidSchemaManager.class)
            .in(LazySingleton.class);

    Jerseys.addResource(binder, ExternalDruidSchemaResource.class);

    LifecycleModule.register(binder, ExternalDruidSchemaCacheManager.class);
    LifecycleModule.register(binder, ExternalDruidSchemaCacheNotifier.class);
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
      log.error(e, "Got exception while getting node roles.");
      return null;
    }
  }


  @Provides
  @LazySingleton
  public static ExternalDruidSchemaResourceHandler createExternalDruidSchemaResourceHandler(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getResourceHandlerClassForService(nodeRoles));
  }

  @Provides
  @LazySingleton
  public static ExternalDruidSchemaCacheManager createExternalDruidSchemaCacheManager(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getCacheManagerClassForService(nodeRoles));
  }

  @Provides
  @LazySingleton
  public static ExternalDruidSchemaCacheNotifier createExternalDruidSchemaCacheNotifier(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getCacheNotifierClassForService(nodeRoles));
  }

  private static Class<? extends ExternalDruidSchemaResourceHandler> getResourceHandlerClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorExternalDruidSchemaResourceHandler.class;
    } else if (isBrokerRole(nodeRoles)) {
      return BrokerExternalDruidSchemaResourceHandler.class;
    } else {
      return DefaultExternalDruidSchemaResourceHandler.class;
    }
  }

  private static Class<? extends ExternalDruidSchemaCacheManager> getCacheManagerClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorExternalDruidSchemaCacheManager.class;
    } else if (isBrokerRole(nodeRoles)) {
      return CoordinatorPollingExternalDruidSchemaCacheManager.class;
    } else {
      return DefaultExternalDruidSchemaCacheManager.class;
    }
  }

  private static Class<? extends ExternalDruidSchemaCacheNotifier> getCacheNotifierClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorExternalDruidSchemaCacheNotifier.class;
    } else {
      return NoopExternalDruidSchemaCacheNotifier.class;
    }
  }

  public static boolean isBrokerRole(Set<NodeRole> nodeRoles)
  {
    if (nodeRoles == null) {
      return false;
    }
    // this assumes that a node will never be a broker + coordinator, which is true for now
    return nodeRoles.contains(NodeRole.BROKER);
  }

  public static boolean isCoordinatorRole(Set<NodeRole> nodeRoles)
  {
    if (nodeRoles == null) {
      return false;
    }
    return nodeRoles.contains(NodeRole.COORDINATOR);
  }
}
