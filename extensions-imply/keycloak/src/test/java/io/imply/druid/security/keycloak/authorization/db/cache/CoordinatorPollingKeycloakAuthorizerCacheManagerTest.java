/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.ISE;
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
  private Injector injector;
  private AuthorizerMapper authorizerMapper;
  private KeycloakAuthCommonCacheConfig commonCacheConfig;
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());
  private DruidLeaderClient druidLeaderClient;

  private CoordinatorPollingKeycloakAuthorizerCacheManager target;

  @Before
  public void setup() throws IOException, InterruptedException
  {

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


    target = Mockito.spy(new CoordinatorPollingKeycloakAuthorizerCacheManager(
        injector,
        commonCacheConfig,
        objectMapper,
        druidLeaderClient
    ));
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
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);
    Mockito.when(responseHolder.getContent()).thenReturn(roleMapDeserialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertEquals(ROLE_MAP, target.getRoleMap());

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test
  public void test_start_keycloakAuthorizerConfiguredAndCacheDirectory_doesPollCoordinatorAndWriteToDisk()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);
    Mockito.when(responseHolder.getContent()).thenReturn(roleMapDeserialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertEquals(ROLE_MAP, target.getRoleMap());

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
    File expectedCachedRoleMapFile = new File("/tmp/cache", "keycloakAuthorizer.role.cache");
    Mockito.verify(target, Mockito.times(2)).writeFileAtomically(expectedCachedRoleMapFile, roleMapDeserialized);
  }

  @Test
  public void test_start_keycloakAuthorizerConfiguredAndCacheDirectoryAndNullRolesFromCoordinator_doesPollCoordinatorAndDoesNotWriteToDisk()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(null);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertTrue(target.getRoleMap().isEmpty());

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(target, Mockito.never()).writeFileAtomically(ArgumentMatchers.any(), ArgumentMatchers.any());
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
    target.handleAuthorizerRoleUpdate(roleMapDeserialized);
    Assert.assertEquals(ROLE_MAP, target.getRoleMap());

    Mockito.verify(target, Mockito.never()).writeFileAtomically(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test
  public void test_handleAuthorizerRoleUpdate_cacheDirectoryNotNull_updatesCachedMapAndDoesWriteToDisk()
      throws IOException
  {
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    byte[] roleMapDeserialized = objectMapper.writeValueAsBytes(ROLE_MAP);

    target.start();
    target.handleAuthorizerRoleUpdate(roleMapDeserialized);
    Assert.assertEquals(ROLE_MAP, target.getRoleMap());

    File expectedCachedRoleMapFile = new File(commonCacheConfig.getCacheDirectory(), "keycloakAuthorizer.role.cache");
    Mockito.verify(target).writeFileAtomically(expectedCachedRoleMapFile, roleMapDeserialized);
  }

  @Test
  public void test_oadRoleMapFromDisk_fileDoesNotExist_returnNull() throws IOException
  {
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/bogus/cache");
    Assert.assertNull(target.loadRoleMapFromDisk());
  }
}
