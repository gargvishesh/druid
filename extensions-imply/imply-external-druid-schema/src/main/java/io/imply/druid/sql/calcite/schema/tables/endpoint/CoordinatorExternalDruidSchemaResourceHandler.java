/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.endpoint;

import com.google.inject.Inject;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.cache.ExternalDruidSchemaCacheManager;

import javax.ws.rs.core.Response;
import java.util.Map;

public class CoordinatorExternalDruidSchemaResourceHandler implements ExternalDruidSchemaResourceHandler
{
  private final ExternalDruidSchemaCacheManager cacheManager;

  @Inject
  public CoordinatorExternalDruidSchemaResourceHandler(
      ExternalDruidSchemaCacheManager cacheManager
  )
  {
    this.cacheManager = cacheManager;
  }

  @Override
  public Response tableSchemaUpdateListener(byte[] serializedSchemaMap)
  {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @Override
  public Response getCachedTableSchemaMaps()
  {
    Map<String, TableSchema> schemaMap = cacheManager.getTableSchemas();
    return Response.ok(schemaMap).build();
  }
}
