/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.net.URI;
import java.util.Objects;

@Path("/druid-ext/view-manager/v1/views")
@ResourceFilters(ConfigResourceFilter.class)
public class ViewStateManagerResource
{
  private static final Logger LOG = new Logger(ViewStateManagerResource.class);
  private final ViewStateManager viewStateManager;

  @Inject
  public ViewStateManagerResource(
      final ViewStateManager viewStateManager
  )
  {
    this.viewStateManager = viewStateManager;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getViews(
      @Context final HttpServletRequest req
  )
  {
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(req.getHeader(HttpHeaders.Names.ACCEPT));
    if (isSmile) {
      // if smile, just stream cached bytes
      return Response.ok()
                     .entity((StreamingOutput) output -> output.write(viewStateManager.getViewStateSerialized()))
                     .build();
    } else {
      // if plain json, just serialize as normal
      return Response.ok().entity(viewStateManager.getViewState()).build();
    }
  }

  @POST
  @Path("/{name}")
  @Consumes({MediaType.APPLICATION_JSON})
  public Response createView(
      ViewDefinitionRequest viewDefinitionRequest,
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
      int updated = viewStateManager.createView(new ImplyViewDefinition(name, viewDefinitionRequest.getViewSql()));
      if (updated != 1) {
        // how can this be?
        return serverError("createView did not update 1 row.");
      }
      return Response.created(URI.create("")).build();
    }
    catch (ViewAlreadyExistsException alreadyExistsException) {
      return clientError(alreadyExistsException.getMessage());
    }
    catch (IllegalArgumentException iae) {
      return clientError(iae.getMessage());
    }
    catch (Exception ex) {
      LOG.error(ex, "Failed to create view");
      return serverError(ex.getMessage());
    }
  }

  @PUT
  @Path("/{name}")
  @Consumes({MediaType.APPLICATION_JSON})
  public Response alterView(
      ViewDefinitionRequest viewDefinitionRequest,
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
      int updated = viewStateManager.alterView(new ImplyViewDefinition(name, viewDefinitionRequest.getViewSql()));
      if (updated != 1) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok().build();
    }
    catch (IllegalArgumentException iae) {
      return clientError(iae.getMessage());
    }
    catch (Exception ex) {
      LOG.error(ex, "Failed to alter view");
      return serverError(ex.getMessage());
    }
  }

  @DELETE
  @Path("/{name}")
  public Response deleteView(
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
      int deleted = viewStateManager.deleteView(name, null);
      if (deleted != 1) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok().build();
    }
    catch (IllegalArgumentException iae) {
      return clientError(iae.getMessage());
    }
    catch (Exception ex) {
      LOG.error(ex, "Failed to delete view");
      return serverError(ex.getMessage());
    }
  }

  private static Response clientError(String message)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.of("error", message == null ? "unknown error" : message))
                   .build();
  }
  private static Response serverError(String message)
  {
    return Response.serverError()
                   .entity(ImmutableMap.of("error", message == null ? "unknown error" : message))
                   .build();
  }

  static class ViewDefinitionRequest
  {
    private final String viewSql;

    @JsonCreator
    public ViewDefinitionRequest(
        @JsonProperty("viewSql") String viewSql
    )
    {
      this.viewSql = viewSql;
    }

    @JsonProperty
    public String getViewSql()
    {
      return viewSql;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ViewDefinitionRequest that = (ViewDefinitionRequest) o;
      return viewSql.equals(that.viewSql);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(viewSql);
    }

    @Override
    public String toString()
    {
      return "ViewDefinitionRequest{" +
             "viewSql='" + viewSql + '\'' +
             '}';
    }
  }
}
