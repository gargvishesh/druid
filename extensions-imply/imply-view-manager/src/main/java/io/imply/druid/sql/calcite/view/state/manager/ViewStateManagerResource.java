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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.sql.http.SqlQuery;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@Path("/druid-ext/view-manager/v1/views")
@ResourceFilters(ConfigResourceFilter.class)
public class ViewStateManagerResource
{
  private static final Logger LOG = new Logger(ViewStateManagerResource.class);
  private final ViewStateManagementConfig viewStateManagementConfig;
  private final ViewStateManager viewStateManager;
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public ViewStateManagerResource(
      final ViewStateManagementConfig viewStateManagementConfig,
      final ViewStateManager viewStateManager,
      final DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient,
      ObjectMapper jsonMapper
  )
  {
    this.viewStateManagementConfig = viewStateManagementConfig;
    this.viewStateManager = viewStateManager;
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
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
  @Produces(MediaType.APPLICATION_JSON)
  public Response createView(
      ViewDefinitionRequest viewDefinitionRequest,
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return clientError(iae.getMessage());
    }

    try {
      if (!validateViewDefinition(viewDefinitionRequest.getViewSql())) {
        return clientError("Invalid view definition: " + viewDefinitionRequest.getViewSql());
      }
    }
    catch (ViewDefinitionValidationUtils.ClientValidationException ce) {
      return clientError(ce.getMessage());
    }
    catch (Exception e) {
      LOG.error(e, "Encountered exception while validating view definition.");
      return serverError(e.getMessage());
    }

    try {
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
    catch (Exception ex) {
      LOG.error(ex, "Failed to create view");
      return serverError(ex.getMessage());
    }
  }

  @PUT
  @Path("/{name}")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces(MediaType.APPLICATION_JSON)
  public Response alterView(
      ViewDefinitionRequest viewDefinitionRequest,
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return clientError(iae.getMessage());
    }

    try {
      if (!validateViewDefinition(viewDefinitionRequest.getViewSql())) {
        return clientError("Invalid view definition: " + viewDefinitionRequest.getViewSql());
      }
    }
    catch (ViewDefinitionValidationUtils.ClientValidationException ce) {
      return clientError(ce.getMessage());
    }
    catch (Exception e) {
      LOG.error(e, "Encountered exception while validating view definition.");
      return serverError(e.getMessage());
    }

    try {
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
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteView(
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return clientError(iae.getMessage());
    }

    try {
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

  private boolean validateViewDefinition(String viewSql)
  {
    // pick a random broker
    DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeRole(NodeRole.BROKER);
    Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
    List<DiscoveryDruidNode> nodeList = ImmutableList.copyOf(nodes);
    if (nodeList.isEmpty()) {
      throw new RE("Could not discover any brokers when validating view definition[%s]", viewSql);
    }
    Random rng = ThreadLocalRandom.current();
    int indexToChoose = rng.nextInt(nodeList.size());
    DiscoveryDruidNode chosenBroker = nodeList.get(indexToChoose);

    URL listenerURL = getQueryUrl(chosenBroker.getDruidNode());

    Request req = new Request(
        HttpMethod.POST,
        listenerURL
    );
    SqlQuery query = new SqlQuery("EXPLAIN PLAN FOR " + viewSql, null, false, null, null);
    byte[] serializedEntity;
    try {
      serializedEntity = jsonMapper.writeValueAsBytes(query);
      req.setContent(MediaType.APPLICATION_JSON, serializedEntity);
    }
    catch (JsonProcessingException jpe) {
      throw new RE(jpe, "encountered error while serializing view definition validation query[%s]", query);
    }

    // block on the response
    ListenableFuture<StatusResponseHolder> future = httpClient.go(
        req,
        StatusResponseHandler.getInstance(),
        null
    );

    try {
      StatusResponseHolder responseHolder = future.get();
      if (HttpResponseStatus.OK.equals(responseHolder.getStatus())) {
        if (!viewStateManagementConfig.isAllowUnrestrictedViews()) {
          ViewDefinitionValidationUtils.QueryPlanAndResources queryPlanAndResources =
              ViewDefinitionValidationUtils.getQueryPlanAndResourcesFromExplainResponse(
                  responseHolder.getContent(),
                  jsonMapper
              );
          ViewDefinitionValidationUtils.validateQueryPlanAndResources(queryPlanAndResources, jsonMapper);
        }
        return true;
      } else if (HttpResponseStatus.BAD_REQUEST.equals(responseHolder.getStatus())) {
        return false;
      } else {
        throw new RE(
            "Received non-OK response while validating view definition[%s], status[%s]",
            viewSql,
            responseHolder.getStatus()
        );
      }
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RE(e, "encountered error while validating view definition[%s]", viewSql);
    }
  }

  private URL getQueryUrl(DruidNode druidNode)
  {
    try {
      return new URL(
          druidNode.getServiceScheme(),
          druidNode.getHost(),
          druidNode.getPortToUse(),
          "/druid/v2/sql/"
      );
    }
    catch (MalformedURLException mue) {
      LOG.error("ViewStateManagerResource : Malformed SQL query URL for DruidNode[%s]", druidNode);
      throw new RuntimeException(mue);
    }
  }
}
