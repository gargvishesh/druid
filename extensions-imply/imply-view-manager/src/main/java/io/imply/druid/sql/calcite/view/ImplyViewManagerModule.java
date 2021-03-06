/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.listener.BrokerViewStateListener;
import io.imply.druid.sql.calcite.view.state.listener.NoViewStateListener;
import io.imply.druid.sql.calcite.view.state.listener.ViewStateListener;
import io.imply.druid.sql.calcite.view.state.listener.ViewStateListenerResource;
import io.imply.druid.sql.calcite.view.state.manager.CoordinatorViewStateManager;
import io.imply.druid.sql.calcite.view.state.manager.NoViewStateManager;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManager;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource;
import io.imply.druid.sql.calcite.view.state.manager.sql.ViewStateSqlMetadataConnectorTableConfig;
import io.imply.druid.sql.calcite.view.state.notifier.CoordinatorViewStateNotifier;
import io.imply.druid.sql.calcite.view.state.notifier.NoViewStateNotifier;
import io.imply.druid.sql.calcite.view.state.notifier.ViewStateNotifier;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.view.ViewManager;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ImplyViewManagerModule implements DruidModule
{
  private static final Logger log = new Logger(ImplyViewManagerModule.class);

  public static final String TYPE = "imply";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.sql.viewmanager.imply", ViewStateManagementConfig.class);
    JsonConfigProvider.bind(binder, "druid.sql.viewmanager.tables", ViewStateSqlMetadataConnectorTableConfig.class);

    PolyBind.optionBinder(binder, Key.get(ViewManager.class))
            .addBinding(TYPE)
            .to(ImplyViewManager.class)
            .in(LazySingleton.class);

    Jerseys.addResource(binder, ViewStateManagerResource.class);
    Jerseys.addResource(binder, ViewStateListenerResource.class);

    LifecycleModule.register(binder, ViewStateNotifier.class);
    LifecycleModule.register(binder, ViewStateManager.class);
    LifecycleModule.register(binder, ViewStateListener.class);
  }

  @Provides
  @LazySingleton
  public static ViewStateNotifier createViewStateNotifier(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getNotifierClassForService(nodeRoles));
  }

  @Provides
  @LazySingleton
  public static ViewStateManager createViewStateManager(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getStateManagerClassForService(nodeRoles));
  }

  @Provides
  @LazySingleton
  public static ViewStateListener createViewStateListener(
      final Injector injector
  )
  {
    Set<NodeRole> nodeRoles = getNodeRoles(injector);
    return injector.getInstance(getStateListenerClassForService(nodeRoles));
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
    // this assumes that a node will never be a broker + coordinator, which is true for now
    return nodeRoles.contains(NodeRole.COORDINATOR);
  }

  private static Class<? extends ViewStateNotifier> getNotifierClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorViewStateNotifier.class;
    } else {
      return NoViewStateNotifier.class;
    }
  }

  private static Class<? extends ViewStateManager> getStateManagerClassForService(Set<NodeRole> nodeRoles)
  {
    if (isCoordinatorRole(nodeRoles)) {
      return CoordinatorViewStateManager.class;
    } else {
      return NoViewStateManager.class;
    }
  }

  private static Class<? extends ViewStateListener> getStateListenerClassForService(Set<NodeRole> nodeRoles)
  {
    if (isBrokerRole(nodeRoles)) {
      return BrokerViewStateListener.class;
    } else {
      return NoViewStateListener.class;
    }
  }
}
