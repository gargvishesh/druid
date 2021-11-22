/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.cache;

import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * This class is reponsible for maintaining a cache of the table schemas received from the SaaS tables service.
 */
public interface ExternalDruidSchemaCacheManager
{
  /**
   * Update this cache manager's local state with fresh role information pushed by the coordinator.
   */
  void updateTableSchemas(byte[] serializedSchemaMap);

  /**
   * Return the cache manager's local view of the map of table name to table schemas.
   */
  @Nullable
  Map<String, TableSchema> getTableSchemas();
}
