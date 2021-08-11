/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.state.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.TokenService;
import io.imply.druid.security.keycloak.authorization.state.notifier.KeycloakAuthorizerCacheNotifier;
import io.imply.druid.security.keycloak.cache.PollingManagedCache;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.keycloak.adapters.KeycloakDeployment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Polls the keycloak server using {@link TokenService#getClientNotBefore()} to fetch 'not-before' policy information,
 * to facilitate token revocation.
 */
public class KeycloakClientNotBeforePollingCache extends PollingManagedCache<Map<String, Integer>>
{
  private static final EmittingLogger LOG = new EmittingLogger(KeycloakClientNotBeforePollingCache.class);

  private final KeycloakDeployment keycloakDeployment;
  private final KeycloakAuthorizerCacheNotifier notifier;
  private final TokenService keycloakTokenService;
  private volatile ConcurrentHashMap<String, Integer> cachedMap;
  private volatile byte[] cacheBytes;

  public KeycloakClientNotBeforePollingCache(
      String cacheName,
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      ObjectMapper objectMapper,
      KeycloakDeployment deployment,
      KeycloakAuthorizerCacheNotifier notifier
  )
  {
    super(cacheName, commonCacheConfig, objectMapper);
    this.keycloakDeployment = deployment;
    this.notifier = notifier;
    this.keycloakTokenService = new TokenService(keycloakDeployment, Collections.emptyMap(), Collections.emptyMap());
    this.cachedMap = new ConcurrentHashMap<>();

    if (commonCacheConfig.isEnableCacheNotifications()) {
      this.notifier.setNotBeforeUpdateSource(
          () -> cacheBytes
      );
    }
  }


  @Override
  protected void initializeCacheValue()
  {
    fetchData(null, true);
  }

  @Override
  protected void refreshCacheValue()
  {
    fetchData(null, false);
  }

  @Nullable
  @Override
  public Map<String, Integer> getCacheValue()
  {
    return cachedMap;
  }

  @Override
  protected TypeReference<Map<String, Integer>> getTypeReference()
  {
    return new TypeReference<Map<String, Integer>>(){};
  }

  @Override
  public Map<String, Integer> handleUpdate(byte[] serializedData)
  {
    cacheBytes = serializedData;
    try {
      Map<String, Integer> data = objectMapper.readValue(
          serializedData,
          getTypeReference()
      );
      if (data != null) {
        cachedMap = new ConcurrentHashMap<>(data);
      }

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeCachedDataToDisk(serializedData);
      }
      notifier.setNotBeforeUpdateSource(() -> serializedData);
      notifier.scheduleNotBeforeUpdate();
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize fresh data from keycloak").emit();
    }
    return cachedMap;
  }

  @Nullable
  @Override
  protected byte[] tryFetchDataForPath(String path) throws Exception
  {
    TokenService.ClientTokenNotBeforeResponse response = keycloakTokenService.getClientNotBefore();
    return objectMapper.writeValueAsBytes(response.getNotBeforePolicies());
  }
}
