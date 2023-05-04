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
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.emitter.ExtraServiceDimensions;

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
      extraDims.addBinding("task_id").toProvider(new Provider<String>()
      {
        @Inject
        private Injector injector;


        @Override
        public String get()
        {
          return injector.getInstance(Task.class).getId();
        }
      });

      extraDims.addBinding("group_id").toProvider(new Provider<String>()
      {
        @Inject
        private Injector injector;


        @Override
        public String get()
        {
          return injector.getInstance(Task.class).getGroupId();
        }
      });

      extraDims.addBinding("data_source").toProvider(new Provider<String>()
      {
        @Inject
        private Injector injector;

        @Override
        public String get()
        {
          return injector.getInstance(Task.class).getDataSource();
        }
      });
    }
  }
}
