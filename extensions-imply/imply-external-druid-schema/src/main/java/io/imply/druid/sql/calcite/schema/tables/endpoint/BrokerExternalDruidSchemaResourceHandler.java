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
import io.imply.druid.sql.calcite.schema.tables.state.cache.ExternalDruidSchemaCacheManager;
import org.apache.druid.java.util.common.logger.Logger;

import javax.ws.rs.core.Response;

public class BrokerExternalDruidSchemaResourceHandler implements ExternalDruidSchemaResourceHandler
{
  private static final Logger LOG = new Logger(BrokerExternalDruidSchemaResourceHandler.class);

  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  private final ExternalDruidSchemaCacheManager cache;

  @Inject
  public BrokerExternalDruidSchemaResourceHandler(
      ExternalDruidSchemaCacheManager cacheManager
  )
  {
    this.cache = cacheManager;
  }

  @Override
  public Response tableSchemaUpdateListener(byte[] serializedSchemaMap)
  {
    cache.updateTableSchemas(serializedSchemaMap);
    return Response.ok().build();
  }

  @Override
  public Response getCachedTableSchemaMaps()
  {
    return NOT_FOUND_RESPONSE;
  }
}
