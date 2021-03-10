/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.ImplyViewManager;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.ViewStateUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class BrokerViewStateListener implements ViewStateListener
{
  public static final String VIEW_STATE_SNAPSHOT_FILENAME = "imply.view_state";
  private static final EmittingLogger LOG = new EmittingLogger(BrokerViewStateListener.class);

  private final ObjectMapper objectMapper;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidLeaderClient druidLeaderClient;
  private final ViewStateManagementConfig commonCacheConfig;
  private final ViewManager viewManager;
  private final ScheduledExecutorService exec;

  private final Map<String, ImplyViewDefinition> viewCache = new HashMap<>();

  private final PlannerFactory plannerFactory;

  private volatile byte[] viewCacheSerialized;

  @Inject
  public BrokerViewStateListener(
      ViewStateManagementConfig commonCacheConfig,
      ViewManager viewManager,
      final PlannerFactory plannerFactory,
      @Smile ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.exec = Execs.scheduledSingleThreaded("BrokerViewStateListener-Exec--%d");
    this.commonCacheConfig = commonCacheConfig;
    this.viewManager = viewManager;
    this.objectMapper = objectMapper;
    this.druidLeaderClient = druidLeaderClient;
    this.plannerFactory = plannerFactory;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    LOG.info("Starting BrokerViewStateListener.");

    try {
      updateViewStateFromCoordinator(true);

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting view state random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled view state poll is running");

              updateViewStateFromCoordinator(false);

              LOG.debug("Scheduled view state poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for view state.").emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Started BrokerViewStateListener.");
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

    LOG.info("BrokerViewStateListener is stopping.");
    exec.shutdown();
    LOG.info("BrokerViewStateListener is stopped.");
  }

  public Map<String, ImplyViewDefinition> getViewState()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return viewCache;
  }

  public byte[] getViewStateSerialized()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return viewCacheSerialized;
  }

  @Override
  public void setViewState(byte[] serializedViewState)
  {
    LOG.debug("Received view state update");
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {
      Map<String, ImplyViewDefinition> newViewState = ViewStateUtils.deserializeViewMap(
          objectMapper,
          serializedViewState
      );

      updateInternalViewState(newViewState, serializedViewState);

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeViewStateToDisk(serializedViewState);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize view state received from coordinator.").emit();
    }
  }

  @VisibleForTesting
  protected void updateViewStateFromCoordinator(boolean isInit)
  {
    try {
      RetryUtils.retry(
          () -> {
            tryUpdateViewStateFromCoordinator();
            return true;
          },
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching view state").emit();
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load view state snapshot from disk.");
            loadViewStateFromDisk();
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading view state snapshot from disk.")
               .emit();
          }
        }
      }
    }
  }

  @VisibleForTesting
  protected File getViewStateSnapshotFile()
  {
    return new File(commonCacheConfig.getCacheDirectory(), VIEW_STATE_SNAPSHOT_FILENAME);
  }

  @Nullable
  private Map<String, ImplyViewDefinition> loadViewStateFromDisk() throws IOException
  {
    File viewStateFile = getViewStateSnapshotFile();
    if (!viewStateFile.exists()) {
      return null;
    }
    byte[] viewStateBytes = Files.readAllBytes(viewStateFile.toPath());
    Map<String, ImplyViewDefinition> newViewState = ViewStateUtils.deserializeViewMap(objectMapper, viewStateBytes);

    updateInternalViewState(newViewState, viewStateBytes);

    return newViewState;
  }

  @VisibleForTesting
  protected void writeViewStateToDisk(byte[] viewMapBytes) throws IOException
  {
    File viewMapFile = getViewStateSnapshotFile();
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    cacheDir.mkdirs();
    FileUtils.writeAtomically(
        viewMapFile,
        out -> {
          out.write(viewMapBytes);
          return null;
        }
    );
  }

  @VisibleForTesting
  protected void tryUpdateViewStateFromCoordinator() throws Exception
  {
    Map<String, ImplyViewDefinition> viewMap;
    Request req =
        druidLeaderClient.makeRequest(HttpMethod.GET, "/druid-ext/view-manager/v1/views")
                         .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE);
    BytesFullResponseHolder responseHolder = druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    if (!HttpResponseStatus.OK.equals(responseHolder.getStatus())) {
      LOG.error(
          "Failed to get view state from coordinator, response code [%s], content [%s]",
          responseHolder.getStatus(),
          StringUtils.fromUtf8(responseHolder.getContent())
      );
      return;
    }
    byte[] viewMapBytes = responseHolder.getContent();
    if (ArrayUtils.isNotEmpty(viewMapBytes)) {
      viewMap = ViewStateUtils.deserializeViewMap(objectMapper, viewMapBytes);

      updateInternalViewState(viewMap, viewMapBytes);

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeViewStateToDisk(viewMapBytes);
      }

    } else {
      LOG.info("Empty cached serialized view state retrieved");
    }
  }

  private void updateInternalViewState(Map<String, ImplyViewDefinition> viewMap, byte[] viewMapBytes)
  {
    synchronized (viewCache) {
      viewCache.clear();
      if (viewManager instanceof ImplyViewManager) {
        // this is sort of ugly, it might be better to either push reset to ViewManager interface, or try to
        // merge the updated cache into the existing view set?
        ((ImplyViewManager) viewManager).reset();

        viewCacheSerialized = viewMapBytes;
        for (Map.Entry<String, ImplyViewDefinition> view : viewMap.entrySet()) {
          viewCache.put(view.getKey(), view.getValue());
          viewManager.createView(plannerFactory, view.getKey(), view.getValue().getViewSql());
        }
      }
    }
  }
}
