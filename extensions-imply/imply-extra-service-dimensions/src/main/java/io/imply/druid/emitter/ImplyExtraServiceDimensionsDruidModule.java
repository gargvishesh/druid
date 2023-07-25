/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.emitter;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.emitter.ExtraServiceDimensions;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;

import java.util.Set;

/**
 * A Druid module that injects extra service dimensions using {@link ExtraServiceDimensions}
 */
public class ImplyExtraServiceDimensionsDruidModule implements DruidModule
{

  private Set<NodeRole> nodeRoles;

  @Inject
  public void setNodeRoles(@Self Set<NodeRole> nodeRoles)
  {
    this.nodeRoles = nodeRoles;
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder<String, String> extraDims = MapBinder.newMapBinder(
        binder,
        String.class,
        String.class,
        ExtraServiceDimensions.class
    );


    // add information related to tasks for peons
    if (nodeRoles.contains(NodeRole.PEON)) {
      extraDims.addBinding("polaris_task_id").toProvider(new Provider<String>()
      {
        @Inject
        private DataSourceTaskIdHolder dataSourceTaskIdHolderProvider;

        @Override
        public String get()
        {
          return dataSourceTaskIdHolderProvider.getTaskId();
        }
      });

      extraDims.addBinding("polaris_group_id").toProvider(new Provider<String>()
      {
        @Inject
        private Task task;

        @Override
        public String get()
        {
          return task.getGroupId();
        }
      });

      extraDims.addBinding("polaris_data_source").toProvider(new Provider<String>()
      {
        @Inject
        private DataSourceTaskIdHolder task;

        @Override
        public String get()
        {
          return task.getDataSource();
        }
      });

      // these extra dimensions aren't easily addable to the http emitter like they are added
      // to clarity emitter or statsd emitter. so we are adding them as system properties unfortunately.
      // ideally we add the ability to add dimensions to the emitter for parse exceptions/log but that would
      // take too long
      extraDims.addBinding("polaris_org_id").toInstance(System.getProperty("druid.imply.polaris.org.id"));
      extraDims.addBinding("polaris_org_name").toInstance(System.getProperty("druid.imply.polaris.org.name"));
      extraDims.addBinding("polaris_env").toInstance(System.getProperty("druid.imply.polaris.env"));
      if (!"empty".equals(System.getProperty("druid.imply.polaris.project.name", "empty"))) {
        extraDims.addBinding("polaris_project_name")
                 .toInstance(System.getProperty("druid.imply.polaris.project.name"));
      }
      if (!"empty".equals(System.getProperty("druid.imply.polaris.project.id", "empty"))) {
        extraDims.addBinding("polaris_project_id")
                 .toInstance(System.getProperty("druid.imply.polaris.project.id"));
      }
    }
  }
}
