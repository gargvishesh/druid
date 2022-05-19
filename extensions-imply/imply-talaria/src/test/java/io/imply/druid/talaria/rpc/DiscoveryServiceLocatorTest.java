/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class DiscoveryServiceLocatorTest
{
  private static final DiscoveryDruidNode NODE1 = new DiscoveryDruidNode(
      new DruidNode("test-service", "node1.example.com", false, -1, 8888, false, true),
      NodeRole.BROKER,
      Collections.emptyMap()
  );

  private static final DiscoveryDruidNode NODE2 = new DiscoveryDruidNode(
      new DruidNode("test-service", "node2.example.com", false, -1, 8888, false, true),
      NodeRole.BROKER,
      Collections.emptyMap()
  );

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  public DruidNodeDiscoveryProvider discoveryProvider;

  private DiscoveryServiceLocator locator;

  @After
  public void tearDown()
  {
    if (locator != null) {
      locator.close();
    }
  }

  @Test
  public void test_locate_initializeEmpty() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assert.assertFalse(future.isDone());

    discovery.fire(DruidNodeDiscovery.Listener::nodeViewInitialized);
    Assert.assertEquals(ServiceLocations.forLocations(Collections.emptySet()), future.get());
  }

  @Test
  public void test_locate_initializeNonEmpty() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assert.assertFalse(future.isDone());

    discovery.fire(listener -> {
      listener.nodesAdded(ImmutableSet.of(NODE1));
      listener.nodesAdded(ImmutableSet.of(NODE2));
      listener.nodeViewInitialized();
    });

    Assert.assertEquals(
        ServiceLocations.forLocations(
            ImmutableSet.of(
                ServiceLocation.fromDruidNode(NODE1.getDruidNode()),
                ServiceLocation.fromDruidNode(NODE2.getDruidNode())
            )
        ),
        future.get()
    );
  }

  @Test
  public void test_locate_removeAfterAdd() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    discovery.fire(listener -> {
      listener.nodesAdded(ImmutableSet.of(NODE1));
      listener.nodesAdded(ImmutableSet.of(NODE2));
      listener.nodeViewInitialized();
      listener.nodesRemoved(ImmutableSet.of(NODE1));
    });

    Assert.assertEquals(
        ServiceLocations.forLocations(
            ImmutableSet.of(
                ServiceLocation.fromDruidNode(NODE2.getDruidNode())
            )
        ),
        locator.locate().get()
    );
  }

  @Test
  public void test_locate_closed() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    final ListenableFuture<ServiceLocations> future = locator.locate();
    locator.close();

    Assert.assertEquals(ServiceLocations.closed(), future.get()); // Call made prior to close()
    Assert.assertEquals(ServiceLocations.closed(), locator.locate().get()); // Call made after close()

    Assert.assertEquals(0, discovery.getListeners().size());
  }

  private static class TestDiscovery implements DruidNodeDiscovery
  {
    @GuardedBy("this")
    private final List<Listener> listeners;

    public TestDiscovery()
    {
      listeners = new ArrayList<>();
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
      listeners.add(listener);
    }

    @Override
    public synchronized void removeListener(Listener listener)
    {
      listeners.remove(listener);
    }

    public synchronized List<Listener> getListeners()
    {
      return ImmutableList.copyOf(listeners);
    }

    public synchronized void fire(Consumer<Listener> f)
    {
      for (final Listener listener : listeners) {
        f.accept(listener);
      }
    }
  }
}
