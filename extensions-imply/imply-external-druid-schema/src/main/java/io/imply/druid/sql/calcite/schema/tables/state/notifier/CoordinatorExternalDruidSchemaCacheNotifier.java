/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.notifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.cache.CommonCacheNotifier;
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
public class CoordinatorExternalDruidSchemaCacheNotifier implements ExternalDruidSchemaCacheNotifier
{
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CommonCacheNotifier cacheSchemaNotifier;

  @Inject
  public CoordinatorExternalDruidSchemaCacheNotifier(
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient,
      ImplyExternalDruidSchemaCommonCacheConfig cacheConfig
  )
  {
    cacheSchemaNotifier = new CommonCacheNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        "/druid-ext/imply-external-druid-schema/listen/schemas",
        "CoordinatorExternalDruidSchemaCacheNotifier"
    );
  }

  @VisibleForTesting
  public CoordinatorExternalDruidSchemaCacheNotifier()
  {
    cacheSchemaNotifier = new CommonCacheNotifier();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      cacheSchemaNotifier.start();
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
      cacheSchemaNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void setSchemaUpdateSource(Supplier<byte[]> schemaMap)
  {
    cacheSchemaNotifier.setUpdateSource(schemaMap);
  }

  @Override
  public void scheduleSchemaUpdate()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    cacheSchemaNotifier.scheduleUpdate();
  }
}
