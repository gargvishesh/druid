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
import com.google.inject.Inject;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.authorization.state.notifier.KeycloakAuthorizerCacheNotifier;
import io.imply.druid.security.keycloak.authorization.state.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.cache.PollingCacheManager;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.representations.adapters.config.AdapterConfig;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Runs on the coordinator, a sort of leader for the keycloak caches. This cache manager then acts as a provider of
 * this state for authentication/authorization processing when performed by a Coordinator.
 *
 * Uses {@link KeycloakAuthorizerMetadataStorageUpdater} to provide 'role' information, direct from the metadata source,
 * and periodically polls the keycloak server with {@link KeycloakClientNotBeforePollingCache} to provide keycloak
 * 'not-before' policy information.
 *
 * This class bundles a {@link KeycloakAuthorizerCacheNotifier} to push notifications to the rest of the cluster
 * for any keycloak 'not-before' policy information updates.
 *
 * For non-coordinators, see {@link CoordinatorPollingKeycloakAuthorizerCacheManager}
 *
 */
@ManageLifecycle
public class CoordinatorKeycloakAuthorizerCacheManager extends PollingCacheManager implements KeycloakAuthorizerCacheManager
{
  private final KeycloakAuthorizerMetadataStorageUpdater roleStorage;
  private final KeycloakClientNotBeforePollingCache notBeforeCache;

  @Inject
  public CoordinatorKeycloakAuthorizerCacheManager(
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      @EscalatedGlobal AdapterConfig escalatedConfig,
      KeycloakAuthorizerMetadataStorageUpdater roleStorage,
      KeycloakAuthorizerCacheNotifier notifier,
      @Smile ObjectMapper smileMapper
  )
  {
    super(commonCacheConfig);

    this.roleStorage = roleStorage;
    this.notBeforeCache = new KeycloakClientNotBeforePollingCache(
        "not-before",
        commonCacheConfig,
        smileMapper,
        KeycloakDeploymentBuilder.build(escalatedConfig),
        notifier
    );
    this.addCache(notBeforeCache);
  }

  @Override
  public void updateRoles(byte[] serializedRoleMap)
  {
    // ignore, we are the leader
  }

  @Nullable
  @Override
  public Map<String, KeycloakAuthorizerRole> getRoles()
  {
    return roleStorage.getCachedRoleMap();
  }

  @Override
  public void updateNotBefore(byte[] serializeNotBeforeMap)
  {
    // ignore, we are the leader
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

  @Override
  public String getCacheManagerName()
  {
    return "keycloak";
  }

  @Override
  public boolean shouldStart()
  {
    return true;
  }
}
