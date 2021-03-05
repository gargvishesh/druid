/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.google.inject.Inject;
import org.apache.calcite.schema.TableMacro;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.view.DruidViewMacro;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ImplyViewManager implements ViewManager
{
  private final Escalator escalator;
  private final DruidViewMacroFactory druidViewMacroFactory;
  private final ConcurrentMap<String, DruidViewMacro> views;

  @Inject
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
    throw new UnsupportedOperationException("alterView not supported");
  }

  @Override
  public void dropView(String viewName)
  {
    final TableMacro oldValue = views.remove(viewName);
    if (oldValue == null) {
      throw new ISE("View[%s] does not exist", viewName);
    }
  }

  @Override
  public Map<String, DruidViewMacro> getViews()
  {
    return views;
  }
}
