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
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import io.imply.druid.sql.calcite.schema.cache.PollingManagedCache;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.notifier.ExternalDruidSchemaCacheNotifier;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.net.MalformedURLException;
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
      ImplyExternalDruidSchemaCommonConfig commonConfig,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      ExternalDruidSchemaCacheNotifier notifier
  )
  {
    super(cacheName, commonConfig, objectMapper);
    this.notifier = notifier;
    this.httpClient = httpClient;
    this.cachedMap = new ConcurrentHashMap<>();

    if (commonConfig.isEnableCacheNotifications()) {
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

      if (commonConfig.getCacheDirectory() != null) {
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
          new Request(HttpMethod.GET, getTablesSchemasURL()),
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

  private URL getTablesSchemasURL()
  {
    // Use the new URL config if present, otherwise fallback to the old config.
    try {
      return commonConfig.getTablesSchemasUrl() != null ?
             new URL(commonConfig.getTablesSchemasUrl()) : new URL(commonConfig.getTablesServiceUrl());
    }
    catch (MalformedURLException mue) {
      String errContext = StringUtils.format("Malformed table schema URL specified - tablesSchemasUrl: %s, tablesServiceUrl: %s",
                                             commonConfig.getTablesSchemasUrl(), commonConfig.getTablesServiceUrl());
      LOG.error(mue, errContext);
      throw new RuntimeException(errContext, mue);
    }
  }
}
