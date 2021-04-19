/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.listener;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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

/**
 * Stay awhile, and listen...
 */
@Path("/druid/v1/views/listen")
@ResourceFilters(ConfigResourceFilter.class)
public class ViewStateListenerResource
{
  private static final Logger LOG = new Logger(ViewStateManagerResource.class);
  private final ViewStateListener viewStateListener;

  @Inject
  public ViewStateListenerResource(
      final ViewStateListener viewStateListener
  )
  {
    this.viewStateListener = viewStateListener;
  }

  @POST
  @Produces({SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response updateCache(
      InputStream in,
      @Context HttpServletRequest req
  )
  {
    try {
      viewStateListener.setViewState(ByteStreams.toByteArray(in));
    }
    catch (IOException e) {
      LOG.error(e, "Failed to update view cache");
      return serverError(e.getMessage());
    }
    return Response.ok().build();
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getViews(
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    // internal API
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return serverError(iae.getMessage());
    }

    ImplyViewDefinition targetView = viewStateListener.getViewState().get(name);
    if (targetView == null) {
      return clientNotFoundError(StringUtils.format("View[%s] not found.", name));
    }

    return Response.ok().entity(targetView).build();
  }

  private static Response clientNotFoundError(String message)
  {
    return Response.status(Response.Status.NOT_FOUND)
                   .entity(ImmutableMap.of("error", message == null ? "unknown error" : message))
                   .build();
  }

  private static Response serverError(String message)
  {
    return Response.serverError()
                   .entity(ImmutableMap.of("error", message == null ? "unknown error" : message))
                   .build();
  }
}
