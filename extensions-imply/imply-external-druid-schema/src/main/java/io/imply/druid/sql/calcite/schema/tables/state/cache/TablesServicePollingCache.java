/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.cache.PollingManagedCache;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.notifier.ExternalDruidSchemaCacheNotifier;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Polls the SaaS tables service to fetch table schema information.
 */
public class TablesServicePollingCache extends PollingManagedCache<Map<String, TableSchema>>
{
  private static final EmittingLogger LOG = new EmittingLogger(TablesServicePollingCache.class);

  private final HttpClient httpClient;
  private final ExternalDruidSchemaCacheNotifier notifier;
  private volatile ConcurrentHashMap<String, TableSchema> cachedMap;
  private volatile byte[] cacheBytes;

  public TablesServicePollingCache(
      String cacheName,
      ImplyExternalDruidSchemaCommonCacheConfig commonCacheConfig,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      ExternalDruidSchemaCacheNotifier notifier
  )
  {
    super(cacheName, commonCacheConfig, objectMapper);
    this.notifier = notifier;
    this.httpClient = httpClient;
    this.cachedMap = new ConcurrentHashMap<>();

    if (commonCacheConfig.isEnableCacheNotifications()) {
      this.notifier.setSchemaUpdateSource(
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
  public Map<String, TableSchema> getCacheValue()
  {
    return cachedMap;
  }

  @Override
  protected TypeReference<Map<String, TableSchema>> getTypeReference()
  {
    return new TypeReference<Map<String, TableSchema>>(){};
  }

  @Override
  public Map<String, TableSchema> handleUpdate(byte[] serializedData)
  {
    cacheBytes = serializedData;
    try {
      Map<String, TableSchema> data = objectMapper.readValue(
          serializedData,
          getTypeReference()
      );
      if (data != null) {
        cachedMap = new ConcurrentHashMap<>(data);
      }

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeCachedDataToDisk(serializedData);
      }
      notifier.setSchemaUpdateSource(() -> serializedData);
      notifier.scheduleSchemaUpdate();
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize fresh data from tables service").emit();
    }
    return cachedMap;
  }

  @Nullable
  @Override
  protected byte[] tryFetchDataForPath(String path) throws Exception
  {
    try {
      final InputStream is = httpClient.go(
          new Request(HttpMethod.GET, new URL(commonCacheConfig.getTablesServiceUrl())),
          new InputStreamResponseHandler()
      ).get();
      return IOUtils.toByteArray(is);
    }
    catch (InterruptedException e) {
      LOG.warn(e, "Got InterruptedException when fetching table schemas.");
      return null;
    }
    catch (ExecutionException e) {
      // Unwrap if possible
      LOG.warn(e, "Got ExecutionException when fetching table schemas.");
      return null;
    }
  }
}
