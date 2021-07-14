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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.cache.CoordinatorPollingMapCache;
import io.imply.druid.security.keycloak.cache.PollingCacheManager;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Doesn't run on the coordinator, but does run on all other Druid node types. Periodically polls the coordinator to
 * to update the Keycloak role to permission mapping cache and the 'not-before' policy cache, which are both
 * {@link CoordinatorPollingMapCache}.
 *
 * This cache manager then acts as a provider of this state for authentication/authorization processing
 * on non-coordinators. For Coordinators see {@link CoordinatorKeycloakAuthorizerCacheManager}
 */
@ManageLifecycle
public class CoordinatorPollingKeycloakAuthorizerCacheManager extends PollingCacheManager implements KeycloakAuthorizerCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingKeycloakAuthorizerCacheManager.class);

  private final Injector injector;

  private final CoordinatorPollingMapCache<KeycloakAuthorizerRole> roleCache;
  private final CoordinatorPollingMapCache<Integer> notBeforeCache;

  @Inject
  public CoordinatorPollingKeycloakAuthorizerCacheManager(
      Injector injector,
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper smileMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    super(commonCacheConfig);
    this.injector = injector;

    this.roleCache = makeRoleCache(commonCacheConfig, smileMapper, druidLeaderClient);
    this.notBeforeCache = makeNotBeforeCache(commonCacheConfig, smileMapper, druidLeaderClient);
    this.addCache(roleCache);
    this.addCache(notBeforeCache);
  }

  @VisibleForTesting
  public CoordinatorPollingKeycloakAuthorizerCacheManager(
      Injector injector,
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      CoordinatorPollingMapCache<KeycloakAuthorizerRole> roleCache,
      CoordinatorPollingMapCache<Integer> notBeforeCache
  )
  {
    super(commonCacheConfig);
    this.injector = injector;

    this.roleCache = roleCache;
    this.notBeforeCache = notBeforeCache;
    this.addCache(roleCache);
    this.addCache(notBeforeCache);
  }

  @Override
  public String getCacheManagerName()
  {
    return "keycloak";
  }

  @Override
  public boolean shouldStart()
  {
    boolean isKeycloakAuthorizerConfigured = isKeycloakAuthorizerConfigured();
    if (!isKeycloakAuthorizerConfigured) {
      LOG.warn("Did not find any keycloak authorizer confgured. Not polling coordinator for roleMap updates");
    }
    return isKeycloakAuthorizerConfigured;
  }


  private boolean isKeycloakAuthorizerConfigured()
  {
    AuthorizerMapper authorizerMapper = injector.getInstance(AuthorizerMapper.class);

    if (authorizerMapper == null || authorizerMapper.getAuthorizerMap() == null) {
      return false;
    }

    for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
      Authorizer authorizer = entry.getValue();
      if (authorizer instanceof ImplyKeycloakAuthorizer) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void updateRoles(byte[] serializedRoleMap)
  {
    roleCache.handleUpdate(serializedRoleMap);
  }

  @Nullable
  @Override
  public Map<String, KeycloakAuthorizerRole> getRoles()
  {
    return roleCache.getCacheValue();
  }

  @Override
  public void updateNotBefore(byte[] serializeNotBeforeMap)
  {
    notBeforeCache.handleUpdate(serializeNotBeforeMap);
  }

  @Nullable
  @Override
  public Map<String, Integer> getNotBefore()
  {
    return notBeforeCache.getCacheValue();
  }

  @Override
  public boolean validateNotBeforePolicies()
  {
    return commonCacheConfig.isEnforceNotBeforePolicies();
  }

  @VisibleForTesting
  static CoordinatorPollingMapCache<KeycloakAuthorizerRole> makeRoleCache(
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      ObjectMapper smileMapper,
      DruidLeaderClient druidLeaderClient
  )
  {
    return new CoordinatorPollingMapCache<>(
        commonCacheConfig,
        smileMapper,
        druidLeaderClient,
        "role-permissions",
        "/druid-ext/keycloak-security/authorization/cachedSerializedRoleMap",
        KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
    );
  }

  @VisibleForTesting
  static CoordinatorPollingMapCache<Integer> makeNotBeforeCache(
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      ObjectMapper smileMapper,
      DruidLeaderClient druidLeaderClient
  )
  {
    return new CoordinatorPollingMapCache<>(
        commonCacheConfig,
        smileMapper,
        druidLeaderClient,
        "not-before",
        "/druid-ext/keycloak-security/authorization/cachedSerializedNotBeforeMap",
        KeycloakAuthUtils.AUTHORIZER_NOT_BEFORE_TYPE_REFERENCE
    );
  }

}
