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
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
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
      return Response.serverError().entity(ImmutableMap.of("error", e.getMessage())).build();
    }
    return Response.ok().build();
  }
}
