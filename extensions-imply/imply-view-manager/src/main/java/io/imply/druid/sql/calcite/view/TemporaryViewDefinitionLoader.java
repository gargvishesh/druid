/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.io.File;
import java.io.IOException;

/**
 * This is a temporary implementation, used for demo purposes. It will be removed once the following are implemented:
 * https://implydata.atlassian.net/browse/IMPLY-5777
 * https://implydata.atlassian.net/browse/IMPLY-5778
 */
@ManageLifecycle
public class TemporaryViewDefinitionLoader
{
  private static final File VIEW_DEFINITIONS_FILE = new File("/tmp/imply_views.json");
  private final PlannerFactory plannerFactory;
  private final ObjectMapper jsonMapper;
  private final ViewManager viewManager;
  private ImplyViewCacheUpdateMessage views;
  private File fileToUse;

  private final Object lock = new Object();
  private volatile boolean started = false;

  @Inject
  public TemporaryViewDefinitionLoader(
      final ObjectMapper jsonMapper,
      final PlannerFactory plannerFactory,
      final ViewManager viewManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.plannerFactory = plannerFactory;
    this.viewManager = viewManager;
    this.fileToUse = VIEW_DEFINITIONS_FILE;
  }

  @VisibleForTesting
  public void setFileToUse(File fileToUse)
  {
    this.fileToUse = fileToUse;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      try {
        views = jsonMapper.readValue(fileToUse, ImplyViewCacheUpdateMessage.class);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      for (ImplyViewDefinition viewDefinition : views.getViews()) {
        viewManager.createView(plannerFactory, viewDefinition.getViewName(), viewDefinition.getViewSql());
      }
      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }
      started = false;
    }
  }
}
