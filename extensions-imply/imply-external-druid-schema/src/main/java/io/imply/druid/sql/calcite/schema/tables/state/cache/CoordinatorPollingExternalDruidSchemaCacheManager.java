/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.schema.ExternalDruidSchemaUtils;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.cache.CoordinatorPollingMapCache;
import io.imply.druid.sql.calcite.schema.cache.PollingCacheManager;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Doesn't run on the coordinator, but does run on the broker. Periodically polls the coordinator to
 * to update the table schemas, which is a {@link CoordinatorPollingMapCache}.
 *
 * This cache manager then acts as a provider of this state the Druid SQL layer.
 */
@ManageLifecycle
public class CoordinatorPollingExternalDruidSchemaCacheManager extends PollingCacheManager implements
    ExternalDruidSchemaCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingExternalDruidSchemaCacheManager.class);

  private final CoordinatorPollingMapCache<TableSchema> schemaCache;

  @Inject
  public CoordinatorPollingExternalDruidSchemaCacheManager(
      ImplyExternalDruidSchemaCommonCacheConfig commonCacheConfig,
      ObjectMapper jsonMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    super(commonCacheConfig);
    this.schemaCache = makeSchemaCache(commonCacheConfig, jsonMapper, druidLeaderClient);
    this.addCache(schemaCache);
  }

  @Override
  public String getCacheManagerName()
  {
    return "table-schemas";
  }

  @Override
  public boolean shouldStart()
  {
    return true;
  }

  @Override
  public void updateTableSchemas(byte[] serializedSchemaMap)
  {
    schemaCache.handleUpdate(serializedSchemaMap);
  }

  @Nullable
  @Override
  public Map<String, TableSchema> getTableSchemas()
  {
    return schemaCache.getCacheValue();
  }

  @VisibleForTesting
  static CoordinatorPollingMapCache<TableSchema> makeSchemaCache(
      ImplyExternalDruidSchemaCommonCacheConfig commonCacheConfig,
      ObjectMapper jsonMapper,
      DruidLeaderClient druidLeaderClient
  )
  {
    return new CoordinatorPollingMapCache<>(
        commonCacheConfig,
        jsonMapper,
        druidLeaderClient,
        "table-schemas",
        "/druid-ext/imply-external-druid-schema/cachedSerializedSchemaMap",
        ExternalDruidSchemaUtils.TABLE_SCHEMA_MAP_TYPE_REFERENCE
    );
  }

  @VisibleForTesting
  public CoordinatorPollingExternalDruidSchemaCacheManager(
      ImplyExternalDruidSchemaCommonCacheConfig commonCacheConfig,
      CoordinatorPollingMapCache<TableSchema> schemaCache
  )
  {
    super(commonCacheConfig);
    this.schemaCache = schemaCache;
    this.addCache(schemaCache);
  }
}
