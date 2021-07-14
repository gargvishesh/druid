/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.state.notifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.cache.CommonCacheNotifier;
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
public class CoordinatorKeycloakAuthorizerCacheNotifier implements KeycloakAuthorizerCacheNotifier
{
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CommonCacheNotifier cacheRoleNotifier;
  private final CommonCacheNotifier cacheNotBeforeNotifier;

  @Inject
  public CoordinatorKeycloakAuthorizerCacheNotifier(
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient,
      KeycloakAuthCommonCacheConfig cacheConfig
  )
  {
    cacheRoleNotifier = new CommonCacheNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        "/druid-ext/keycloak-security/authorization/listen/roles",
        "CoordinatorKeycloakAuthorizerCacheNotifier"
    );
    cacheNotBeforeNotifier = new CommonCacheNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        "/druid-ext/keycloak-security/authorization/listen/not-before",
        "CoordinatorKeycloakAuthorizerCacheNotifier"
    );
  }

  @VisibleForTesting
  public CoordinatorKeycloakAuthorizerCacheNotifier()
  {
    cacheRoleNotifier = new CommonCacheNotifier();
    cacheNotBeforeNotifier = new CommonCacheNotifier();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      cacheRoleNotifier.start();
      cacheNotBeforeNotifier.start();
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
      cacheRoleNotifier.stop();
      cacheNotBeforeNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void setRoleUpdateSource(Supplier<byte[]> roleMap)
  {
    cacheRoleNotifier.setUpdateSource(roleMap);
  }

  @Override
  public void scheduleRoleUpdate()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    cacheRoleNotifier.scheduleUpdate();
  }

  @Override
  public void setNotBeforeUpdateSource(Supplier<byte[]> updateSource)
  {
    cacheNotBeforeNotifier.setUpdateSource(updateSource);
  }

  @Override
  public void scheduleNotBeforeUpdate()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    cacheNotBeforeNotifier.scheduleUpdate();
  }
}
