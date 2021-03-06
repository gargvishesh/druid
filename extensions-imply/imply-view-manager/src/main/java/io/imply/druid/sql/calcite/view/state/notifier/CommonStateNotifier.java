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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

  // TODO: to address https://implydata.atlassian.net/browse/IMPLY-6212, we should move to a queueless system
  private final BlockingQueue<byte[]> updateQueue;

  public CommonStateNotifier(
      ViewStateManagementConfig viewCacheConfig,
      DruidNodeDiscoveryProvider discoveryProvider,
      HttpClient httpClient,
      String baseUrl
  )
  {
    this.exec = Execs.singleThreaded("CommonStateNotifier-notifierThread-%d");
    this.updateQueue = new LinkedBlockingQueue<>();
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
              LOG.debug("Waiting for cache update notification");
              byte[] update = updateQueue.take();

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

  public void addUpdate(byte[] updatedItemData)
  {
    if (!viewCacheConfig.isEnableCacheNotifications()) {
      return;
    }

    updateQueue.add(updatedItemData);
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
            new ResponseHandler(),
            Duration.millis(viewCacheConfig.getCacheNotificationTimeout())
        );
        futures.add(future);
      }
    }
    return futures;
  }

  @Nullable
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
      LOG.error("Malformed url for DruidNode[%s] and baseUrl[%s]", druidNode, baseUrl);
      return null;
    }
  }

  // Based off StatusResponseHandler, but with response content ignored
  private static class ResponseHandler implements HttpResponseHandler<StatusResponseHolder, StatusResponseHolder>
  {
    protected static final Logger log = new Logger(ResponseHandler.class);

    @Override
    public ClientResponse<StatusResponseHolder> handleResponse(HttpResponse response, TrafficCop trafficCop)
    {
      return ClientResponse.unfinished(
          new StatusResponseHolder(
              response.getStatus(),
              null
          )
      );
    }

    @Override
    public ClientResponse<StatusResponseHolder> handleChunk(
        ClientResponse<StatusResponseHolder> response,
        HttpChunk chunk,
        long chunkNum
    )
    {
      return response;
    }

    @Override
    public ClientResponse<StatusResponseHolder> done(ClientResponse<StatusResponseHolder> response)
    {
      return ClientResponse.finished(response.getObj());
    }

    @Override
    public void exceptionCaught(ClientResponse<StatusResponseHolder> clientResponse, Throwable e)
    {
      // Its safe to Ignore as the ClientResponse returned in handleChunk were unfinished
      log.error(e, "exceptionCaught in CommonCacheNotifier ResponseHandler.");
    }
  }
}
