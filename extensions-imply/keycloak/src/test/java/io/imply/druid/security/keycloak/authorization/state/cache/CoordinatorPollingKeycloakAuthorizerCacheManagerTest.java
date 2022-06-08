/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.state.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.cache.CoordinatorPollingMapCache;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.regex.Pattern;

public class CoordinatorPollingKeycloakAuthorizerCacheManagerTest
{
  private static final Map<String, KeycloakAuthorizerRole> ROLE_MAP = ImmutableMap.of(
      "role",
      new KeycloakAuthorizerRole(
          "role",
          ImmutableList.of(
              new KeycloakAuthorizerPermission(
                  new ResourceAction(
                      new Resource(
                          "resource1",
                          ResourceType.DATASOURCE
                      ),
                      Action.READ
                  ),
                  Pattern.compile("resource1")
              ))
      )
  );
  private static final Map<String, Integer> NOT_BEFORE_MAP = ImmutableMap.of("some-client", 0);

  private Injector injector;
  private AuthorizerMapper authorizerMapper;
  private KeycloakAuthCommonCacheConfig commonCacheConfig;
  private ObjectMapper objectMapper;
  private DruidLeaderClient druidLeaderClient;

  private CoordinatorPollingKeycloakAuthorizerCacheManager target;
  private CoordinatorPollingMapCache<KeycloakAuthorizerRole> roleCache;
  private CoordinatorPollingMapCache<Integer> notBeforeCache;

  @Before
  public void setup() throws IOException, InterruptedException
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    objectMapper = new DefaultObjectMapper(smileFactory, null);
    objectMapper.getFactory().setCodec(objectMapper);
    injector = Mockito.mock(Injector.class);
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of(
        "authorizer", new ImplyKeycloakAuthorizer()
    ));
    Mockito.when(injector.getInstance(AuthorizerMapper.class)).thenReturn(authorizerMapper);

    commonCacheConfig = Mockito.mock(KeycloakAuthCommonCacheConfig.class);
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(200_000L);
    Mockito.when(commonCacheConfig.getMaxSyncRetries()).thenReturn(1);

    druidLeaderClient = Mockito.mock(DruidLeaderClient.class);
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(null);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);

    roleCache = Mockito.spy(
        CoordinatorPollingKeycloakAuthorizerCacheManager.makeRoleCache(
            commonCacheConfig,
            objectMapper,
            druidLeaderClient
        )
    );
    notBeforeCache = Mockito.spy(
        CoordinatorPollingKeycloakAuthorizerCacheManager.makeNotBeforeCache(
            commonCacheConfig,
            objectMapper,
            druidLeaderClient
        )
    );
    target = Mockito.spy(
        new CoordinatorPollingKeycloakAuthorizerCacheManager(
            injector,
          commonCacheConfig,
          roleCache,
          notBeforeCache
      )
    );
  }

  @Test
  public void test_start_noKeycloakAuthorizerConfigurd_doesNotPollCoordinator()
      throws IOException, InterruptedException
  {
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(null);
    Mockito.when(injector.getInstance(AuthorizerMapper.class)).thenReturn(authorizerMapper);
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Mockito.verify(druidLeaderClient, Mockito.never()).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.never()).go(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test
  public void test_start_keycloakAuthorizerConfigured_doesPollCoordinator()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Request roleRequest = Mockito.mock(Request.class);
    Request notBeforeRequest = Mockito.mock(Request.class);
    Mockito.when(roleRequest.getUrl()).thenReturn(new URL("http://localhost:8081/druid-ext/keycloak-security/authorization/cachedSerializedRoleMap"));
    Mockito.when(notBeforeRequest.getUrl()).thenReturn(new URL("http://localhost:8081/druid-ext/keycloak-security/authorization/cachedSerializedNotBeforeMap"));
    Mockito.when(druidLeaderClient.makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedRoleMap")))).thenReturn(roleRequest);
    Mockito.when(druidLeaderClient.makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedNotBeforeMap")))).thenReturn(notBeforeRequest);
    BytesFullResponseHolder roleResponseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);
    Mockito.when(roleResponseHolder.getContent()).thenReturn(roleMapDeserialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.argThat(req -> req != null && req.getUrl().getPath().contains("cachedSerializedRoleMap")), ArgumentMatchers.any())).thenReturn(roleResponseHolder);
    BytesFullResponseHolder notBeforeResponseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] notBeforeSerialized = objectMapper.writeValueAsBytes(NOT_BEFORE_MAP);
    Mockito.when(notBeforeResponseHolder.getContent()).thenReturn(notBeforeSerialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.argThat(req -> req != null && req.getUrl().getPath().contains("cachedSerializedNotBeforeMap")), ArgumentMatchers.any())).thenReturn(notBeforeResponseHolder);

    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertEquals(ROLE_MAP, target.getRoles());
    Assert.assertEquals(NOT_BEFORE_MAP, target.getNotBefore());

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedRoleMap")));
    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedNotBeforeMap")));
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.argThat(req -> req.getUrl().getPath().contains("cachedSerializedRoleMap")), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.argThat(req -> req.getUrl().getPath().contains("cachedSerializedNotBeforeMap")), ArgumentMatchers.any());
  }

  @Test
  public void test_start_keycloakAuthorizerConfiguredAndCacheDirectory_doesPollCoordinatorAndWriteToDisk()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    Request roleRequest = Mockito.mock(Request.class);
    Request notBeforeRequest = Mockito.mock(Request.class);
    Mockito.when(roleRequest.getUrl()).thenReturn(new URL("http://localhost:8081/druid-ext/keycloak-security/authorization/cachedSerializedRoleMap"));
    Mockito.when(notBeforeRequest.getUrl()).thenReturn(new URL("http://localhost:8081/druid-ext/keycloak-security/authorization/cachedSerializedNotBeforeMap"));
    Mockito.when(druidLeaderClient.makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedRoleMap")))).thenReturn(roleRequest);
    Mockito.when(druidLeaderClient.makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedNotBeforeMap")))).thenReturn(notBeforeRequest);
    BytesFullResponseHolder roleResponseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);
    Mockito.when(roleResponseHolder.getContent()).thenReturn(roleMapDeserialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.argThat(req -> req != null && req.getUrl().getPath().contains("cachedSerializedRoleMap")), ArgumentMatchers.any())).thenReturn(roleResponseHolder);
    BytesFullResponseHolder notBeforeResponseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] notBeforeSerialized = objectMapper.writeValueAsBytes(NOT_BEFORE_MAP);
    Mockito.when(notBeforeResponseHolder.getContent()).thenReturn(notBeforeSerialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.argThat(req -> req != null && req.getUrl().getPath().contains("cachedSerializedNotBeforeMap")), ArgumentMatchers.any())).thenReturn(notBeforeResponseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertEquals(ROLE_MAP, target.getRoles());

    Mockito.verify(druidLeaderClient, Mockito.times(4)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(4)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
    File expectedCachedRoleMapFile = new File("/tmp/cache", "role-permissions.cache");
    Mockito.verify(roleCache, Mockito.times(2)).writeFileAtomically(expectedCachedRoleMapFile, roleMapDeserialized);
  }

  @Test
  public void test_start_keycloakAuthorizerConfiguredAndCacheDirectoryAndNullRolesFromCoordinator_doesPollCoordinatorAndDoesNotWriteToDisk()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    Mockito.when(commonCacheConfig.isEnableCacheNotifications()).thenReturn(true);
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(null);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertNull(target.getRoles());
    Assert.assertNull(target.getNotBefore());

    Mockito.verify(druidLeaderClient, Mockito.times(4)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(4)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(roleCache, Mockito.never()).writeFileAtomically(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test (expected = ISE.class)
  public void test_start_twice_throwsException()
  {
    target.start();
    target.start();
  }

  @Test (expected = IllegalMonitorStateException.class)
  public void test_stop_beforeStart_throwsException()
  {
    target.stop();
  }

  @Test
  public void test_stop_afterStart_succeeds()
  {
    target.start();
    target.stop();
  }

  @Test (expected = ISE.class)
  public void test_stop_twice_throwsException()
  {
    target.start();
    target.stop();
    target.stop();
  }

  @Test
  public void test_handleAuthorizerRoleUpdate_cacheDirectoryNull_updatesCachedMapAndDoesNotWriteToDisk()
      throws IOException
  {
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn(null);
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);

    target.start();
    target.updateRoles(roleMapDeserialized);
    Assert.assertEquals(ROLE_MAP, target.getRoles());

    Mockito.verify(roleCache, Mockito.never()).writeFileAtomically(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test
  public void test_handleAuthorizerRoleUpdate_cacheDirectoryNotNull_updatesCachedMapAndDoesWriteToDisk()
      throws IOException
  {
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);
    byte[] notBeforeMapDeserialized = objectMapper.writeValueAsBytes(NOT_BEFORE_MAP);

    target.start();
    target.updateRoles(roleMapDeserialized);
    target.updateNotBefore(notBeforeMapDeserialized);
    Assert.assertEquals(ROLE_MAP, target.getRoles());
    Assert.assertEquals(NOT_BEFORE_MAP, target.getNotBefore());

    File expectedCachedRoleMapFile = new File(commonCacheConfig.getCacheDirectory(), "role-permissions.cache");
    File expectedCachedNotBeforeMapFile = new File(commonCacheConfig.getCacheDirectory(), "not-before.cache");
    Mockito.verify(roleCache).writeFileAtomically(expectedCachedRoleMapFile, roleMapDeserialized);
    Mockito.verify(notBeforeCache).writeFileAtomically(expectedCachedNotBeforeMapFile, notBeforeMapDeserialized);
  }

  @Test
  public void test_oadRoleMapFromDisk_fileDoesNotExist_returnNull() throws IOException
  {
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/bogus/cache");
    Assert.assertNull(roleCache.readCachedDataFromDisk());
  }
}
