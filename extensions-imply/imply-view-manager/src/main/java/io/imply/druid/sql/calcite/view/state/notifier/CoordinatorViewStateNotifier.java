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
import java.util.function.Supplier;

@ManageLifecycle
public class CoordinatorViewStateNotifier implements ViewStateNotifier
{
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CommonStateNotifier commonStateNotifier;

  @Inject
  public CoordinatorViewStateNotifier(
      ViewStateManagementConfig cacheConfig,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    commonStateNotifier = new CommonStateNotifier(
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
      commonStateNotifier.start();
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
      commonStateNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void setUpdateSource(Supplier<byte[]> updateSource)
  {
    commonStateNotifier.setUpdateSource(updateSource);
  }

  @Override
  public void scheduleUpdate()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    commonStateNotifier.scheduleUpdate();
  }

  @VisibleForTesting
  public void sendUpdateForTests(byte[] mapp)
  {
    commonStateNotifier.sendUpdate(mapp);
  }
}
