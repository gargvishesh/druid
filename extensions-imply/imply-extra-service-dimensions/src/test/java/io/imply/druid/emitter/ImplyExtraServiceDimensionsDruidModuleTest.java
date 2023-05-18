/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.emitter;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.server.emitter.ExtraServiceDimensions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class ImplyExtraServiceDimensionsDruidModuleTest
{
  private static final String TASK_ID = "TASK_ID";
  private static final String GROUP_ID = "GROUP_ID";
  private static final String DATA_SOURCE = "DATA_SOURCE";
  private static final String SERVICE_DIM = "SERVICE_DIM";
  @Mock
  private Task task;
  private Injector injector;
  private ImplyExtraServiceDimensionsDruidModule target;

  @Before
  public void setUp()
  {
    Mockito.when(task.getId()).thenReturn(TASK_ID);
    Mockito.when(task.getGroupId()).thenReturn(GROUP_ID);
    Mockito.when(task.getDataSource()).thenReturn(DATA_SOURCE);
    target = new ImplyExtraServiceDimensionsDruidModule();

  }

  @Test
  public void testPeonInjectsTaskDimensions()
  {
    target.setNodeRoles(ImmutableSet.of(NodeRole.PEON));
    injector = createInjector(ImmutableSet.of(NodeRole.PEON));
    Map<String, String> extraServiceDims =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, String>>()
        {
        }, ExtraServiceDimensions.class));
    Assert.assertEquals(4, extraServiceDims.size());
    Assert.assertEquals(TASK_ID, extraServiceDims.get("task_id"));
    Assert.assertEquals(GROUP_ID, extraServiceDims.get("group_id"));
    Assert.assertEquals(DATA_SOURCE, extraServiceDims.get("data_source"));
    Assert.assertEquals(SERVICE_DIM, extraServiceDims.get("extra_dim"));
  }

  @Test
  public void testMultipleNodeRolesWithPeonInjectsTaskDimensions()
  {
    target.setNodeRoles(ImmutableSet.of(NodeRole.PEON, NodeRole.MIDDLE_MANAGER));
    injector = createInjector(ImmutableSet.of(NodeRole.PEON, NodeRole.MIDDLE_MANAGER));
    Map<String, String> extraServiceDims =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, String>>()
        {
        }, ExtraServiceDimensions.class));
    Assert.assertEquals(4, extraServiceDims.size());
    Assert.assertEquals(TASK_ID, extraServiceDims.get("task_id"));
    Assert.assertEquals(GROUP_ID, extraServiceDims.get("group_id"));
    Assert.assertEquals(DATA_SOURCE, extraServiceDims.get("data_source"));
    Assert.assertEquals(SERVICE_DIM, extraServiceDims.get("extra_dim"));
  }

  @Test
  public void testMultipleNodeRolesWithoutPeonShouldNotInjectTaskDimensions()
  {
    target.setNodeRoles(ImmutableSet.of(NodeRole.COORDINATOR, NodeRole.OVERLORD));
    injector = createInjector(ImmutableSet.of(NodeRole.COORDINATOR, NodeRole.OVERLORD));
    Map<String, String> extraServiceDims =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, String>>()
        {
        }, ExtraServiceDimensions.class));
    Assert.assertEquals(1, extraServiceDims.size());
    Assert.assertNull(extraServiceDims.get("task_id"));
    Assert.assertNull(extraServiceDims.get("group_id"));
    Assert.assertNull(extraServiceDims.get("data_source"));
    Assert.assertEquals(SERVICE_DIM, extraServiceDims.get("extra_dim"));
  }

  private Injector createInjector(Set<NodeRole> nodeRoles)
  {
    return Guice.createInjector(
        target,
        binder -> {
          binder.bind(Task.class).toInstance(task);
          for (NodeRole nodeRole : nodeRoles) {
            Multibinder.newSetBinder(binder, NodeRole.class, Self.class).addBinding().toInstance(nodeRole);
          }
          // Bound from the EmitterModule
          MapBinder.newMapBinder(
              binder,
              String.class,
              String.class,
              ExtraServiceDimensions.class
          ).addBinding("extra_dim").toInstance(SERVICE_DIM);

        }
    );
  }
}
