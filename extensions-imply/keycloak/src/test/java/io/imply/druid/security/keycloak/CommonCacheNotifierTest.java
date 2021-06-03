/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */


package io.imply.druid.security.keycloak;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.joda.time.Duration;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


public class CommonCacheNotifierTest
{
  private static final boolean CACHE_NOTIFICATIONS_ENABLED = true;
  private static final boolean CACHE_NOTIFICATIONS_DISABLED = false;
  private static final long CACHE_NOTIFICATION_TIMEOUT_MILLIS = 1000L;
  private static final long NOTIFICATIER_UPDATE_PERIOD_MILLIS = 1000L;
  private static final String BASE_URL = "/keycloak/";
  private static final String CALLER_NAME = "CALLER_NAME";

  private static final String HTTP_SCHEME = "http";

  private static final byte[] DATA_TO_SEND = StringUtils.toUtf8("Data to send");

  private KeycloakAuthCommonCacheConfig cacheConfig;
  private DruidNodeDiscoveryProvider discoveryProvider;
  private HttpClient httpClient;

  private CommonCacheNotifier cacheNotifier;

  @Test
  public void test_setUpdateSource_cacheNotifictionsEnabled_sendUpdate()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    cacheConfig = Mockito.mock(KeycloakAuthCommonCacheConfig.class);
    discoveryProvider = Mockito.mock(DruidNodeDiscoveryProvider.class, Mockito.RETURNS_DEEP_STUBS);
    httpClient = Mockito.mock(HttpClient.class);
    mockCacheConfig(true);
    cacheNotifier = Mockito.spy(new CommonCacheNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        BASE_URL,
        CALLER_NAME
    ));

    mockDiscoveryProvider();
    mockHttpClient();
    cacheNotifier.setUpdateSource(() -> DATA_TO_SEND);
    cacheNotifier.setUpdateSource(() -> DATA_TO_SEND);
    cacheNotifier.scheduleUpdate();
    cacheNotifier.start();
    Thread.sleep(5000L);
    verifyDataSentToAllExpectedNodeTypes(Mockito.atLeast(1));
  }


  @Test
  public void test_setUpdateSource_twoUpdatesButStoppedAfterFirst_sendOneUpdate()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    cacheConfig = Mockito.mock(KeycloakAuthCommonCacheConfig.class);
    discoveryProvider = Mockito.mock(DruidNodeDiscoveryProvider.class, Mockito.RETURNS_DEEP_STUBS);
    httpClient = Mockito.mock(HttpClient.class);
    mockCacheConfig(true);
    cacheNotifier = Mockito.spy(new CommonCacheNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        BASE_URL,
        CALLER_NAME
    ));

    mockDiscoveryProvider();
    mockHttpClient();
    cacheNotifier.setUpdateSource(() -> DATA_TO_SEND);
    cacheNotifier.scheduleUpdate();
    cacheNotifier.start();
    cacheNotifier.stop();
    cacheNotifier.scheduleUpdate();
    cacheNotifier.setUpdateSource(() -> DATA_TO_SEND);
    waitForShutdown();
    verifyDataSentToAllExpectedNodeTypes(Mockito.atMost(1));
  }

  @Test
  public void test_setUpdateSource_cacheNotifictionsDisabled_doesNotsendUpdate()
  {
    cacheConfig = Mockito.mock(KeycloakAuthCommonCacheConfig.class);
    discoveryProvider = Mockito.mock(DruidNodeDiscoveryProvider.class, Mockito.RETURNS_DEEP_STUBS);
    mockCacheConfig(false);
    cacheNotifier = Mockito.spy(new CommonCacheNotifier(
        cacheConfig,
        discoveryProvider,
        httpClient,
        BASE_URL,
        CALLER_NAME
    ));

    cacheNotifier.start();
    cacheNotifier.setUpdateSource(() -> DATA_TO_SEND);
    cacheNotifier.scheduleUpdate();

    verifyDataNotSentToAnyNodeTypes();
  }

  private void mockCacheConfig(boolean cacheNotificationsEnabled)
  {
    Mockito.when(cacheConfig.isEnableCacheNotifications()).thenReturn(
        cacheNotificationsEnabled ? CACHE_NOTIFICATIONS_ENABLED : CACHE_NOTIFICATIONS_DISABLED);
    Mockito.when(cacheConfig.getCacheNotificationTimeout()).thenReturn(CACHE_NOTIFICATION_TIMEOUT_MILLIS);
    Mockito.when(cacheConfig.getNotifierUpdatePeriod()).thenReturn(NOTIFICATIER_UPDATE_PERIOD_MILLIS);
  }

  private void mockDiscoveryProvider()
  {
    DiscoveryDruidNode brokerDiscoveryNode = Mockito.mock(DiscoveryDruidNode.class, Mockito.RETURNS_DEEP_STUBS);
    DruidNodeDiscovery brokerNodeDiscovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(brokerNodeDiscovery.getAllNodes()).thenReturn(ImmutableList.of(brokerDiscoveryNode));
    DruidNodeDiscovery otherNodeDiscovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(otherNodeDiscovery.getAllNodes()).thenReturn(ImmutableList.of());


    Mockito.when(brokerDiscoveryNode.getDruidNode().getServiceScheme()).thenReturn(HTTP_SCHEME);
    Mockito.when(brokerDiscoveryNode.getDruidNode().getHost()).thenReturn("broker");
    Mockito.when(brokerDiscoveryNode.getDruidNode().getPortToUse()).thenReturn(80);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(brokerNodeDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.OVERLORD)).thenReturn(otherNodeDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.HISTORICAL)).thenReturn(otherNodeDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.PEON)).thenReturn(otherNodeDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.ROUTER)).thenReturn(otherNodeDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.MIDDLE_MANAGER)).thenReturn(otherNodeDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.INDEXER)).thenReturn(otherNodeDiscovery);
  }

  @SuppressWarnings("unchecked")
  private void mockHttpClient() throws InterruptedException, ExecutionException, TimeoutException
  {
    Request request = Mockito.mock(Request.class);
    Mockito.doReturn(request)
           .when(cacheNotifier)
           .createRequest(ArgumentMatchers.any(), ArgumentMatchers.eq(DATA_TO_SEND));
    ListenableFuture<StatusResponseHolder> future = (ListenableFuture<StatusResponseHolder>) Mockito.mock(
        ListenableFuture.class);
    StatusResponseHolder responseHolder = Mockito.mock(StatusResponseHolder.class);
    Mockito.doReturn(ImmutableList.of(responseHolder))
           .when(cacheNotifier)
           .getResponsesFromFutures(ArgumentMatchers.any());
    Mockito.when(httpClient.go(
        ArgumentMatchers.eq(request),
        ArgumentMatchers.any(StatusResponseHandler.class),
        ArgumentMatchers.eq(Duration.millis(CACHE_NOTIFICATION_TIMEOUT_MILLIS))
    )).thenReturn(future);
  }

  private void verifyDataSentToAllExpectedNodeTypes(VerificationMode verificationMode)
  {
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.BROKER);
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.OVERLORD);
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.HISTORICAL);
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.PEON);
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.ROUTER);
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.MIDDLE_MANAGER);
    Mockito.verify(discoveryProvider, verificationMode).getForNodeRole(NodeRole.INDEXER);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.COORDINATOR);

  }

  private void verifyDataNotSentToAnyNodeTypes()
  {
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.BROKER);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.OVERLORD);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.HISTORICAL);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.PEON);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.ROUTER);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.MIDDLE_MANAGER);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.INDEXER);
    Mockito.verify(discoveryProvider, Mockito.never()).getForNodeRole(NodeRole.COORDINATOR);

  }

  private void waitForShutdown()
  {
    TestUtils.retryUntil(
        () -> cacheNotifier.isShutdown(),
        true,
        1000L,
        5,
        "exec thread never shutdown");
  }
}
