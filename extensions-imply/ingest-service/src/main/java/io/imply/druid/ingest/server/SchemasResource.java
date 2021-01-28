/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.metadata.StoredIngestSchema;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Path("/ingest/v1/schemas")
public class SchemasResource
{
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final IngestServiceMetadataStore metadataStore;

  @Inject
  public SchemasResource(
      AuthorizerMapper authorizerMapper,
      IngestServiceMetadataStore metadataStore,
      ObjectMapper jsonMapper
  )
  {
    this.authorizerMapper = authorizerMapper;
    this.metadataStore = metadataStore;
    this.jsonMapper = jsonMapper;
  }

  @POST
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({MediaType.APPLICATION_JSON})
  public Response createSchema(
      InputStream in,
      @Context final HttpServletRequest req
  )
  {
    final IngestSchema ingestSchema;
    try {
      ingestSchema = jsonMapper.readValue(in, IngestSchema.class);
    }
    catch (IOException ioe) {
      return ApiErrors.badRequest(ioe, "Failed to create schema");
    }

    // anyone can make schemas for now
    AuthorizationUtils.authorizeAllResourceActions(
        req,
        ImmutableList.of(),
        authorizerMapper
    );

    Integer newSchemaId;
    try {
      newSchemaId = metadataStore.createSchema(ingestSchema);
      return Response.ok(ImmutableMap.of("schemaId", newSchemaId)).build();
    }
    catch (Exception e) {
      return ApiErrors.serverError(e, "Failed to create schema");
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  public Response getAllSchemas(
      @Context final HttpServletRequest req
  )
  {
    // anyone can read schemas for now
    AuthorizationUtils.authorizeAllResourceActions(
        req,
        ImmutableList.of(),
        authorizerMapper
    );

    try {
      List<StoredIngestSchema> schemaList = metadataStore.getAllSchemas();
      return Response.ok(ImmutableMap.of("schemas", schemaList)).build();
    }
    catch (Exception e) {
      return ApiErrors.serverError(e, "Failed to fetch list of schemas");
    }
  }

  @GET
  @Path("/{schemaId}")
  @Produces({MediaType.APPLICATION_JSON})
  public Response getSchema(
      @PathParam("schemaId") String schemaIdStr,
      @Context final HttpServletRequest req
  )
  {
    // anyone can read schemas for now
    AuthorizationUtils.authorizeAllResourceActions(
        req,
        ImmutableList.of(),
        authorizerMapper
    );

    int schemaId;
    try {
      schemaId = Integer.valueOf(schemaIdStr);
    }
    catch (NumberFormatException nfe) {
      return ApiErrors.badRequest(nfe, nfe.getMessage());
    }

    try {
      StoredIngestSchema ingestSchema = metadataStore.getSchema(schemaId);
      if (ingestSchema == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(ingestSchema).build();
      }
    }
    catch (Exception e) {
      return ApiErrors.serverError(e, "Failed to fetch schema [%s]", schemaId);
    }
  }

  @DELETE
  @Path("/{schemaId}")
  public Response deleteSchema(
      @PathParam("schemaId") String schemaIdStr,
      @Context final HttpServletRequest req
  )
  {
    // anyone can delete schemas for now
    AuthorizationUtils.authorizeAllResourceActions(
        req,
        ImmutableList.of(),
        authorizerMapper
    );

    int schemaId;
    try {
      schemaId = Integer.valueOf(schemaIdStr);
    }
    catch (NumberFormatException nfe) {
      return ApiErrors.badRequest(nfe, nfe.getMessage());
    }

    try {
      int numDeleted = metadataStore.deleteSchema(schemaId);
      if (numDeleted == 0) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok().build();
      }
    }
    catch (Exception e) {
      return ApiErrors.serverError(e, "Failed to delete schema [%s]", schemaId);
    }
  }
}
