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
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.sql.calcite.schema.ExternalDruidSchemaSecurityResourceFilter;
import org.apache.druid.guice.LazySingleton;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid-ext/imply-external-druid-schema")
@LazySingleton
public class ExternalDruidSchemaResource
{
  private final ExternalDruidSchemaResourceHandler resourceHandler;

  @Inject
  public ExternalDruidSchemaResource(
      ExternalDruidSchemaResourceHandler resourceHandler
  )
  {
    this.resourceHandler = resourceHandler;
  }

  /**
   * Listen for update notifications for the schema storage
   */
  @POST
  @Path("/listen/schemas")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ExternalDruidSchemaSecurityResourceFilter.class)
  public Response schemaUpdateListener(
      @Context HttpServletRequest req,
      byte[] serializedSchemaMap
  )
  {
    return resourceHandler.tableSchemaUpdateListener(serializedSchemaMap);
  }

  /**
   * @param req HTTP request
   *
   * @return serialized role map
   */
  @GET
  @Path("/cachedSerializedSchemaMap")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ExternalDruidSchemaSecurityResourceFilter.class)
  public Response getCachedSerializedSchemaMap(
      @Context HttpServletRequest req
  )
  {
    return resourceHandler.getCachedTableSchemaMaps();
  }
}
