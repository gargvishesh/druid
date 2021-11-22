/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.google.inject.Inject;
import io.imply.druid.sql.calcite.schema.tables.state.cache.CoordinatorPollingExternalDruidSchemaCacheManager;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ImplyDruidSchemaManager implements DruidSchemaManager
{
  private final CoordinatorPollingExternalDruidSchemaCacheManager cacheManager;

  @Inject
  public ImplyDruidSchemaManager(
      final CoordinatorPollingExternalDruidSchemaCacheManager cacheManager
  )
  {
    this.cacheManager = cacheManager;
  }

  @Override
  public ConcurrentMap<String, DruidTable> getTables()
  {
    if (cacheManager.getTableSchemas() == null) {
      return new ConcurrentHashMap<>();
    }
    return ExternalDruidSchemaUtils.convertTableSchemasToDruidTables(cacheManager.getTableSchemas());
  }
}
