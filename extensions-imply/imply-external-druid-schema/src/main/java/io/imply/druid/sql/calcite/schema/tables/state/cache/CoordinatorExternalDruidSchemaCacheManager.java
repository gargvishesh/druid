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
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.cache.PollingCacheManager;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.notifier.ExternalDruidSchemaCacheNotifier;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.http.client.HttpClient;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Runs on the coordinator, a sort of leader for the schema caches. This cache manager then acts as a provider of
 * this state for schema processing when performed by a Coordinator.
 *
 * This class bundles a {@link ExternalDruidSchemaCacheNotifier} to push notifications to the rest of the cluster
 * for any schema updates.
 *
 * For non-coordinators, see {@link CoordinatorPollingExternalDruidSchemaCacheManager}
 *
 */
@ManageLifecycle
public class CoordinatorExternalDruidSchemaCacheManager extends PollingCacheManager implements
    ExternalDruidSchemaCacheManager
{
  private final TablesServicePollingCache tablesServiceCache;

  @Inject
  public CoordinatorExternalDruidSchemaCacheManager(
      ImplyExternalDruidSchemaCommonCacheConfig commonCacheConfig,
      ExternalDruidSchemaCacheNotifier notifier,
      ObjectMapper jsonMapper,
      @EscalatedClient HttpClient httpClient
  )
  {
    super(commonCacheConfig);

    this.tablesServiceCache = new TablesServicePollingCache(
        "table-schemas",
        commonCacheConfig,
        jsonMapper,
        httpClient,
        notifier
    );
    this.addCache(tablesServiceCache);
  }

  @Override
  public void updateTableSchemas(byte[] serializedSchemaMap)
  {
    // ignore, we are the leader
  }

  @Nullable
  @Override
  public Map<String, TableSchema> getTableSchemas()
  {
    return tablesServiceCache.getCacheValue();
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

  @VisibleForTesting
  public TablesServicePollingCache getTablesServiceCache()
  {
    return tablesServiceCache;
  }

}
