/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.cache;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Push style notifications that allow propagation of data from whatever server is running this notifier to whoever
 * might be listening.
 */
public class CommonCacheNotifier
{
  private static final EmittingLogger LOG = new EmittingLogger(CommonCacheNotifier.class);

  /**
   * {@link NodeRole#COORDINATOR} is intentionally omitted because it gets its information about the auth state directly
   * from metadata storage.
   */
  private static final List<NodeRole> NODE_TYPES = Arrays.asList(
      NodeRole.BROKER,
      NodeRole.OVERLORD,
      NodeRole.HISTORICAL,
      NodeRole.PEON,
      NodeRole.ROUTER,
      NodeRole.MIDDLE_MANAGER,
      NodeRole.INDEXER
  );

  private final boolean enableCacheNotifications;
  private final long cacheNotificationsTimeout;
  private final long notifierUpdatePeriod;
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final HttpClient httpClient;
  private final String baseUrl;
  private final String callerName;
  private final ExecutorService exec;

  private Supplier<byte[]> updateSource;
  private long newUpdateReceivedTimeMillis = Long.MIN_VALUE;
  private long timeOfLastNotifyMillis = -1;

  public CommonCacheNotifier(
      KeycloakAuthCommonCacheConfig cacheConfig,
      DruidNodeDiscoveryProvider discoveryProvider,
      HttpClient httpClient,
      String baseUrl,
      String callerName
  )
  {
    this.enableCacheNotifications = cacheConfig.isEnableCacheNotifications();
    this.cacheNotificationsTimeout = cacheConfig.getCacheNotificationTimeout();
    this.notifierUpdatePeriod = cacheConfig.getNotifierUpdatePeriod();
    this.exec = Execs.singleThreaded(
        StringUtils.format("%s-notifierThread-", StringUtils.encodeForFormat(callerName)) + "%d"
    );
    this.callerName = callerName;
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.baseUrl = baseUrl;
  }

  @VisibleForTesting
  public CommonCacheNotifier()
  {
    this.enableCacheNotifications = true;
    this.cacheNotificationsTimeout = 1000L;
    this.notifierUpdatePeriod = 1000L;
    this.exec = null;
    this.callerName = null;
    this.discoveryProvider = null;
    this.httpClient = null;
    this.baseUrl = null;
  }

  public void start()
  {
    exec.submit(
        () -> {
          while (!Thread.interrupted()) {
            try {
              if (updateSource == null) {
                // the role state manager hasn't set the update source yet, go back to sleep.
                Thread.sleep(notifierUpdatePeriod);
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
                Thread.sleep(notifierUpdatePeriod);
                continue;
              }

              byte[] serializedMapUpdate = updateSource.get();

              LOG.debug(callerName + ":Sending update notifications");
              // Best effort, if a notification fails, the remote node will eventually poll to update its state
              // We wait for responses however, to avoid flooding remote nodes with notifications.
              List<ListenableFuture<StatusResponseHolder>> futures = sendUpdate(
                  serializedMapUpdate
              );

              try {
                List<StatusResponseHolder> responses = getResponsesFromFutures(futures);

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
                LOG.makeAlert(e, callerName + ":Failed to get response for cache notification.").emit();
              }

              LOG.debug(callerName + ":Received responses for cache update notifications.");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, callerName + ":Error occured while handling updates for cachedRoleMaps.").emit();
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
    if (!enableCacheNotifications) {
      return;
    }
    synchronized (this) {
      newUpdateReceivedTimeMillis = System.currentTimeMillis();
    }
  }

  private List<ListenableFuture<StatusResponseHolder>> sendUpdate(byte[] serializedEntity)
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

        // best effort, if this fails, remote node will poll and pick up the update eventually
        Request req = createRequest(listenerURL, serializedEntity);

        ListenableFuture<StatusResponseHolder> future = httpClient.go(
            req,
            StatusResponseHandler.getInstance(),
            Duration.millis(cacheNotificationsTimeout)
        );
        futures.add(future);
      }
    }
    return futures;
  }

  @VisibleForTesting
  public Request createRequest(
      URL listenerURL,
      byte[] serializedEntity
  )
  {
    Request req = new Request(HttpMethod.POST, listenerURL);
    req.setContent(SmileMediaTypes.APPLICATION_JACKSON_SMILE, serializedEntity);
    return req;
  }

  @VisibleForTesting
  List<StatusResponseHolder> getResponsesFromFutures(
      List<ListenableFuture<StatusResponseHolder>> futures
  ) throws InterruptedException, ExecutionException, TimeoutException
  {
    return Futures.successfulAsList(futures)
                  .get(
                      cacheNotificationsTimeout,
                      TimeUnit.MILLISECONDS
                  );
  }

  @VisibleForTesting
  public boolean isShutdown()
  {
    return exec.isShutdown();
  }

  private URL getListenerURL(DruidNode druidNode, String baseUrl)
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
      LOG.error(callerName + ": Malformed url for DruidNode[%s] and baseUrl[%s]", druidNode, baseUrl);

      throw new RuntimeException(mue);
    }
  }
}
