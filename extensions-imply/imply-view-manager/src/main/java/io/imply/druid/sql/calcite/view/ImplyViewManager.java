/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.calcite.schema.TableMacro;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.view.DruidViewMacro;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ImplyViewManager implements ViewManager
{
  private final Escalator escalator;
  private final DruidViewMacroFactory druidViewMacroFactory;
  private volatile ConcurrentMap<String, DruidViewMacro> views;

  @Inject
  public ImplyViewManager(
      final Escalator escalator,
      final Injector injector
  )
  {
    this.escalator = escalator;
    this.views = new ConcurrentHashMap<>();

    Set<NodeRole> nodeRoles = ImplyViewManagerModule.getNodeRoles(injector);
    if (ImplyViewManagerModule.isBrokerRole(nodeRoles)) {
      this.druidViewMacroFactory = injector.getInstance(DruidViewMacroFactory.class);
      // DruidViewMacroFactory should not be null on brokers
      Preconditions.checkNotNull(druidViewMacroFactory);
    } else {
      // the DruidViewMacroFactory is only available on brokers
      this.druidViewMacroFactory = null;
    }
  }

  @VisibleForTesting
  public ImplyViewManager(
      final Escalator escalator,
      final DruidViewMacroFactory druidViewMacroFactory
  )
  {
    this.escalator = escalator;
    this.druidViewMacroFactory = druidViewMacroFactory;
    this.views = new ConcurrentHashMap<>();
  }

  @Override
  public void createView(
      PlannerFactory plannerFactory,
      String viewName,
      String viewSql
  )
  {
    brokerCheck();
    final TableMacro oldValue =
        views.putIfAbsent(viewName, druidViewMacroFactory.create(plannerFactory, escalator, viewSql));
    if (oldValue != null) {
      throw new ISE("View[%s] already exists", viewName);
    }
  }

  @Override
  public void alterView(
      PlannerFactory plannerFactory,
      String viewName,
      String viewSql
  )
  {
    brokerCheck();
    views.put(viewName, druidViewMacroFactory.create(plannerFactory, escalator, viewSql));
  }

  @Override
  public void dropView(String viewName)
  {
    brokerCheck();
    final TableMacro oldValue = views.remove(viewName);
    if (oldValue == null) {
      throw new ISE("View[%s] does not exist", viewName);
    }
  }

  @Override
  public Map<String, DruidViewMacro> getViews()
  {
    brokerCheck();
    return views;
  }

  public ConcurrentMap<String, DruidViewMacro> generateNewViewsMap(
      PlannerFactory plannerFactory,
      Map<String, ImplyViewDefinition> newViewMap
  )
  {
    ConcurrentMap<String, DruidViewMacro> newViews = new ConcurrentHashMap<>();
    for (Map.Entry<String, ImplyViewDefinition> view : newViewMap.entrySet()) {
      DruidViewMacro newMacro = druidViewMacroFactory.create(plannerFactory, escalator, view.getValue().getViewSql());
      newViews.put(view.getKey(), newMacro);
    }

    return newViews;
  }

  public void swapViewsMap(ConcurrentMap<String, DruidViewMacro> newViewsMap)
  {
    views = newViewsMap;
  }

  private void brokerCheck()
  {
    if (druidViewMacroFactory == null) {
      throw new RuntimeException("ImplyViewManager methods should only be called on brokers.");
    }
  }
}
