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

import javax.ws.rs.core.Response;

public class DefaultExternalDruidSchemaResourceHandler implements ExternalDruidSchemaResourceHandler
{

  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  @Inject
  public DefaultExternalDruidSchemaResourceHandler()
  {
  }

  @Override
  public Response tableSchemaUpdateListener(byte[] serializedSchemaMap)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedTableSchemaMaps()
  {
    return NOT_FOUND_RESPONSE;
  }
}
