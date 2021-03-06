/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.notifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorViewStateNotifier implements ViewStateNotifier
{
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CommonStateNotifier commonCacheNotifier;

  @Inject
  public CoordinatorViewStateNotifier(
      ViewStateManagementConfig cacheConfig,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    commonCacheNotifier = new CommonStateNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        "/druid/v1/views/listen"
    );
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("Can't start.");
    }

    try {
      commonCacheNotifier.start();
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      return;
    }
    try {
      commonCacheNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void propagateViews(byte[] updatedViewMap)
  {
    if (updatedViewMap != null) {
      Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
      commonCacheNotifier.addUpdate(updatedViewMap);
    }
  }

  @VisibleForTesting
  public void sendUpdateForTests(byte[] mapp)
  {
    commonCacheNotifier.sendUpdate(mapp);
  }
}
