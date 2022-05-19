/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link ServiceLocator} that uses {@link DruidNodeDiscovery}.
 */
public class DiscoveryServiceLocator implements ServiceLocator
{
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final NodeRole nodeRole;
  private final DruidNodeDiscovery.Listener listener;

  @GuardedBy("this")
  private boolean started = false;

  @GuardedBy("this")
  private boolean initialized = false;

  @GuardedBy("this")
  private boolean closed = false;

  @GuardedBy("this")
  private final Set<ServiceLocation> locations = new HashSet<>();

  @GuardedBy("this")
  private SettableFuture<ServiceLocations> pendingFuture = null;

  @GuardedBy("this")
  private DruidNodeDiscovery discovery = null;

  public DiscoveryServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider, final NodeRole nodeRole)
  {
    this.discoveryProvider = discoveryProvider;
    this.nodeRole = nodeRole;
    this.listener = new Listener();
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    synchronized (this) {
      if (closed) {
        return Futures.immediateFuture(ServiceLocations.closed());
      } else if (initialized) {
        return Futures.immediateFuture(ServiceLocations.forLocations(ImmutableSet.copyOf(locations)));
      } else {
        if (pendingFuture == null) {
          pendingFuture = SettableFuture.create();
        }

        return Futures.nonCancellationPropagating(pendingFuture);
      }
    }
  }

  @LifecycleStart
  public void start()
  {
    synchronized (this) {
      if (started || closed) {
        throw new ISE("Cannot start once already started or closed");
      } else {
        started = true;
        this.discovery = discoveryProvider.getForNodeRole(nodeRole);
        discovery.registerListener(listener);
      }
    }
  }

  @Override
  @LifecycleStop
  public void close()
  {
    synchronized (this) {
      // Idempotent: can call close() multiple times so long as start() has already been called.
      if (started && !closed) {
        if (discovery != null) {
          discovery.removeListener(listener);
        }

        if (pendingFuture != null) {
          pendingFuture.set(ServiceLocations.closed());
          pendingFuture = null;
        }

        closed = true;
      }
    }
  }

  private class Listener implements DruidNodeDiscovery.Listener
  {
    @Override
    public void nodesAdded(final Collection<DiscoveryDruidNode> nodes)
    {
      synchronized (DiscoveryServiceLocator.this) {
        for (final DiscoveryDruidNode node : nodes) {
          locations.add(ServiceLocation.fromDruidNode(node.getDruidNode()));
        }
      }
    }

    @Override
    public void nodesRemoved(final Collection<DiscoveryDruidNode> nodes)
    {
      synchronized (DiscoveryServiceLocator.this) {
        for (final DiscoveryDruidNode node : nodes) {
          locations.remove(ServiceLocation.fromDruidNode(node.getDruidNode()));
        }
      }
    }

    @Override
    public void nodeViewInitialized()
    {
      synchronized (DiscoveryServiceLocator.this) {
        initialized = true;

        if (pendingFuture != null) {
          pendingFuture.set(ServiceLocations.forLocations(ImmutableSet.copyOf(locations)));
          pendingFuture = null;
        }
      }
    }
  }
}
