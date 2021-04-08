/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.notifier;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class CommonStateNotifier
{
  private static final EmittingLogger LOG = new EmittingLogger(CommonStateNotifier.class);

  /**
   * Only the {@link NodeRole#BROKER} handles views.
   */
  private static final List<NodeRole> NODE_TYPES = ImmutableList.of(
      NodeRole.BROKER
  );

  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final HttpClient httpClient;
  private final ViewStateManagementConfig viewCacheConfig;
  private final String baseUrl;
  private final ExecutorService exec;

  private Supplier<byte[]> updateSource;
  private long newUpdateReceivedTimeMillis = Long.MIN_VALUE;
  private long timeOfLastNotifyMillis = -1;

  public CommonStateNotifier(
      ViewStateManagementConfig viewCacheConfig,
      DruidNodeDiscoveryProvider discoveryProvider,
      HttpClient httpClient,
      String baseUrl
  )
  {
    this.exec = Execs.singleThreaded("CommonStateNotifier-notifierThread-%d");
    this.viewCacheConfig = viewCacheConfig;
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.baseUrl = baseUrl;
  }

  public void start()
  {
    if (!viewCacheConfig.isEnableCacheNotifications()) {
      return;
    }

    exec.submit(
        () -> {
          while (!Thread.interrupted()) {
            try {
              if (updateSource == null) {
                // the view state manager hasn't set the update source yet, go back to sleep.
                Thread.sleep(viewCacheConfig.getNotifierUpdatePeriod());
                continue;
              }

              boolean hasNewUpdate = false;
              synchronized (this) {
                if (newUpdateReceivedTimeMillis >= timeOfLastNotifyMillis) {
                  timeOfLastNotifyMillis = System.currentTimeMillis();
                  hasNewUpdate = true;
                }
              }
              if (!hasNewUpdate) {
                Thread.sleep(viewCacheConfig.getNotifierUpdatePeriod());
                continue;
              }

              byte[] update = updateSource.get();

              LOG.debug("Sending cache update notifications");
              // Best effort, if a notification fails, the remote node will eventually poll to update its state
              // We wait for responses however, to avoid flooding remote nodes with notifications.
              List<ListenableFuture<StatusResponseHolder>> futures = sendUpdate(update);

              try {
                List<StatusResponseHolder> responses = Futures.successfulAsList(futures)
                                                              .get(
                                                                  viewCacheConfig.getCacheNotificationTimeout(),
                                                                  TimeUnit.MILLISECONDS
                                                              );

                for (StatusResponseHolder response : responses) {
                  if (response == null) {
                    LOG.error("Got null future response from update request.");
                  } else if (!HttpResponseStatus.OK.equals(response.getStatus())) {
                    LOG.error("Got error status [%s], content [%s]", response.getStatus(), response.getContent());
                  } else {
                    LOG.debug("Got status [%s]", response.getStatus());
                  }
                }
              }
              catch (Exception e) {
                LOG.makeAlert(e, "Failed to get response for cache notification.").emit();
              }

              LOG.debug("Received responses for cache update notifications.");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while handling updates for cachedUserMaps.").emit();
            }
          }
        }
    );
  }

  public void stop()
  {
    exec.shutdownNow();
  }

  public void setUpdateSource(Supplier<byte[]> updateSource)
  {
    this.updateSource = updateSource;
  }

  public void scheduleUpdate()
  {
    if (!viewCacheConfig.isEnableCacheNotifications()) {
      return;
    }
    synchronized (this) {
      newUpdateReceivedTimeMillis = System.currentTimeMillis();
    }
  }

  @VisibleForTesting
  protected List<ListenableFuture<StatusResponseHolder>> sendUpdate(byte[] serializedEntity)
  {
    List<ListenableFuture<StatusResponseHolder>> futures = new ArrayList<>();
    for (NodeRole nodeRole : NODE_TYPES) {
      DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeRole(nodeRole);
      Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
      for (DiscoveryDruidNode node : nodes) {
        URL listenerURL = getListenerURL(
            node.getDruidNode(),
            baseUrl
        );
        if (listenerURL == null) {
          continue;
        }

        // best effort, if this fails, remote node will poll and pick up the update eventually
        Request req = new Request(
            HttpMethod.POST,
            listenerURL
        );
        req.setContent(SmileMediaTypes.APPLICATION_JACKSON_SMILE, serializedEntity);

        ListenableFuture<StatusResponseHolder> future = httpClient.go(
            req,
            StatusResponseHandler.getInstance(),
            Duration.millis(viewCacheConfig.getCacheNotificationTimeout())
        );
        futures.add(future);
      }
    }
    return futures;
  }

  @Nullable
  public static URL getListenerURL(DruidNode druidNode, String baseUrl)
  {
    try {
      return new URL(
          druidNode.getServiceScheme(),
          druidNode.getHost(),
          druidNode.getPortToUse(),
          baseUrl
      );
    }
    catch (MalformedURLException mue) {
      LOG.error("Malformed url for DruidNode[%s] and baseUrl[%s]", druidNode, baseUrl);
      return null;
    }
  }
}
