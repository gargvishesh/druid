/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.manager.sql.ViewStateSqlMetadataConnector;
import io.imply.druid.sql.calcite.view.state.manager.sql.ViewStateSqlMetadataConnectorTableConfig;
import io.imply.druid.sql.calcite.view.state.notifier.ViewStateNotifier;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorViewStateManager implements ViewStateManager
{
  private static final EmittingLogger LOG =
      new EmittingLogger(CoordinatorViewStateManager.class);

  private final ViewStateSqlMetadataConnector viewStateConnector;
  private final ViewStateManagementConfig cacheConfig;
  private final ViewStateNotifier cacheNotifier;
  private final ObjectMapper smileMapper;
  private final int numRetries = 5;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ScheduledExecutorService exec;
  private volatile boolean stopped = false;

  private volatile Map<String, ImplyViewDefinition> viewCache = new HashMap<>();
  private volatile byte[] viewCacheSerialized;

  @Inject
  public CoordinatorViewStateManager(
      MetadataStorageConnector connector,
      ViewStateSqlMetadataConnectorTableConfig tablesConfig,
      ViewStateManagementConfig cacheConfig,
      ViewStateNotifier cacheNotifier,
      @Smile ObjectMapper smileMapper
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorViewStateManager-Exec--%d");
    if (!(connector instanceof SQLMetadataConnector)) {
      throw new ISE("CoordinatorViewStateManager only works with SQL based metadata store at this time");
    } else {
      this.viewStateConnector = new ViewStateSqlMetadataConnector((SQLMetadataConnector) connector, tablesConfig);
    }
    this.cacheConfig = cacheConfig;
    this.cacheNotifier = cacheNotifier;
    this.smileMapper = smileMapper;

    if (cacheConfig.isEnableCacheNotifications()) {
      this.cacheNotifier.setUpdateSource(
          () -> {
            return viewCacheSerialized;
          }
      );
    }
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      viewStateConnector.init();

      // read the initial view definitions map from metadata storage, if it exists
      try {
        RetryUtils.retry(
            () -> {
              updateViewCacheFromMetadata();
              return true;
            },
            Predicates.alwaysTrue(),
            numRetries
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(cacheConfig.getPollingPeriod()),
          new Duration(cacheConfig.getPollingPeriod()),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              if (stopped) {
                return ScheduledExecutors.Signal.STOP;
              }
              try {
                LOG.debug("Scheduled db view definitions poll is running");
                updateViewCacheFromMetadata();
                LOG.debug("Scheduled db view definitions poll is done");
              }
              catch (Throwable t) {
                LOG.makeAlert(t, "Error occured while polling for view definitions.").emit();
              }
              return ScheduledExecutors.Signal.REPEAT;
            }
          }
      );

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
      throw new ISE("can't stop.");
    }

    LOG.info("CoordinatorViewStateManager is stopping.");
    stopped = true;
    LOG.info("CoordinatorViewStateManager is stopped.");
  }

  @Override
  public int createView(ImplyViewDefinition viewDefinition)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    int updated = viewStateConnector.createView(viewDefinition);
    if (updated > 0) {
      synchronized (this) {
        viewCache.put(viewDefinition.getViewKey(), viewDefinition);
        serializeCache();
      }
    }
    return updated;
  }

  @Override
  public int alterView(ImplyViewDefinition viewDefinition)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    int updated = viewStateConnector.alterView(viewDefinition);
    if (updated > 0) {
      synchronized (this) {
        viewCache.put(viewDefinition.getViewKey(), viewDefinition);
        serializeCache();
      }
    }
    return updated;
  }

  @Override
  public int deleteView(String viewName, @Nullable String viewNamespace)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    int deleted = viewStateConnector.deleteView(viewName, null);
    if (deleted > 0) {
      synchronized (this) {
        viewCache.remove(ImplyViewDefinition.getKey(viewName, viewNamespace));
        serializeCache();
      }
    }
    return deleted;
  }

  @Override
  public Map<String, ImplyViewDefinition> getViewState()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return viewCache;
  }

  @Override
  public byte[] getViewStateSerialized()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return viewCacheSerialized;
  }

  @VisibleForTesting
  protected void updateViewCacheFromMetadata()
  {
    List<ImplyViewDefinition> viewDefinitions = viewStateConnector.getViews();
    synchronized (this) {
      Map<String, ImplyViewDefinition> newViewCache = new HashMap<>();
      for (ImplyViewDefinition definition : viewDefinitions) {
        newViewCache.put(definition.getViewKey(), definition);
      }
      viewCache = newViewCache;
      serializeCache();
    }
  }

  /**
   * Should only be called within a synchronized (this) block
   */
  @GuardedBy("this")
  private void serializeCache()
  {
    try {
      viewCacheSerialized = smileMapper.writeValueAsBytes(viewCache);
      cacheNotifier.scheduleUpdate();
    }
    catch (JsonProcessingException e) {
      throw new ISE(e, "Failed to JSON-Smile serialize cached view definitions");
    }
  }
}
