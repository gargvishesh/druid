/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Specialized {@link CoordinatorPollingCache} whose cache is a {@link Map} with string keys.
 */
public class CoordinatorPollingMapCache<T> extends CoordinatorPollingCache<Map<String, T>>
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingMapCache.class);
  private final TypeReference<Map<String, T>> typeReference;
  private volatile ConcurrentHashMap<String, T> cachedMap;

  public CoordinatorPollingMapCache(
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      ObjectMapper objectMapper,
      DruidLeaderClient coordinatorLeaderClient,
      String cacheName,
      String coordinatorPath,
      TypeReference<Map<String, T>> typeReference
  )
  {
    super(commonCacheConfig, objectMapper, coordinatorLeaderClient, cacheName, coordinatorPath);
    this.typeReference = typeReference;
  }

  @Override
  protected void initializeCacheValue()
  {
    fetchData(coordinatorPath, true);
  }

  @Override
  protected void refreshCacheValue()
  {
    fetchData(coordinatorPath, false);
  }

  @Nullable
  @Override
  public Map<String, T> getCacheValue()
  {
    return cachedMap;
  }

  @Override
  protected TypeReference<Map<String, T>> getTypeReference()
  {
    return typeReference;
  }


  @Override
  public Map<String, T> handleUpdate(byte[] serializedData)
  {
    try {
      Map<String, T> data = objectMapper.readValue(
          serializedData,
          getTypeReference()
      );
      if (data != null) {
        cachedMap = new ConcurrentHashMap<>(data);
      }

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeCachedDataToDisk(serializedData);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize fresh data pushed from coordinator").emit();
    }
    return cachedMap;
  }
}
