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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.notifier.CommonStateNotifier;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
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
import org.joda.time.Duration;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSingleView(
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return clientBadRequestError(iae.getMessage());
    }

    ImplyViewDefinition targetView = viewStateManager.getViewState().get(name);
    if (targetView == null) {
      return clientNotFoundError(StringUtils.format("View[%s] not found.", name));
    }

    return Response.ok().entity(targetView).build();
  }

  @POST
  @Path("/{name}")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({MediaType.APPLICATION_JSON})
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
      return clientBadRequestError(iae.getMessage());
    }

    try {
      if (!validateViewDefinition(viewDefinitionRequest.getViewSql())) {
        return clientBadRequestError("Invalid view definition: " + viewDefinitionRequest.getViewSql());
      }
    }
    catch (ViewDefinitionValidationUtils.ClientValidationException ce) {
      return clientBadRequestError(ce.getMessage());
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
      return clientBadRequestError(alreadyExistsException.getMessage());
    }
    catch (Exception ex) {
      LOG.error(ex, "Failed to create view");
      return serverError(ex.getMessage());
    }
  }

  @PUT
  @Path("/{name}")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({MediaType.APPLICATION_JSON})
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
      return clientBadRequestError(iae.getMessage());
    }

    try {
      if (!validateViewDefinition(viewDefinitionRequest.getViewSql())) {
        return clientBadRequestError("Invalid view definition: " + viewDefinitionRequest.getViewSql());
      }
    }
    catch (ViewDefinitionValidationUtils.ClientValidationException ce) {
      return clientBadRequestError(ce.getMessage());
    }
    catch (Exception e) {
      LOG.error(e, "Encountered exception while validating view definition.");
      return serverError(e.getMessage());
    }

    try {
      int updated = viewStateManager.alterView(new ImplyViewDefinition(name, viewDefinitionRequest.getViewSql()));
      if (updated != 1) {
        return clientNotFoundError(StringUtils.format("View[%s] not found.", name));
      }
      return Response.ok().build();
    }
    catch (IllegalArgumentException iae) {
      return clientBadRequestError(iae.getMessage());
    }
    catch (Exception ex) {
      LOG.error(ex, "Failed to alter view");
      return serverError(ex.getMessage());
    }
  }

  @DELETE
  @Path("/{name}")
  @Produces({MediaType.APPLICATION_JSON})
  public Response deleteView(
      @PathParam("name") String name,
      @Context HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return clientBadRequestError(iae.getMessage());
    }

    try {
      int deleted = viewStateManager.deleteView(name, null);
      if (deleted != 1) {
        return clientNotFoundError(StringUtils.format("View[%s] not found.", name));
      }
      return Response.ok().build();
    }
    catch (IllegalArgumentException iae) {
      return clientBadRequestError(iae.getMessage());
    }
    catch (Exception ex) {
      LOG.error(ex, "Failed to delete view");
      return serverError(ex.getMessage());
    }
  }

  @GET
  @Path("/loadstatus/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getViewLoadStatus(
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    try {
      IdUtils.validateId("view", name);
    }
    catch (IllegalArgumentException iae) {
      return clientBadRequestError(iae.getMessage());
    }

    ImplyViewDefinition targetView = viewStateManager.getViewState().get(name);
    if (targetView == null) {
      return clientNotFoundError(StringUtils.format("View[%s] not found.", name));
    }

    try {
      ViewLoadStatusResponse loadStatusResponse = getViewLoadStatusForCluster(targetView);
      return Response.ok().entity(loadStatusResponse).build();
    }
    catch (Exception e) {
      return serverError(e.getMessage());
    }
  }

  private static Response clientNotFoundError(String message)
  {
    return Response.status(Response.Status.NOT_FOUND)
                   .entity(ImmutableMap.of("error", message == null ? "unknown error" : message))
                   .build();
  }
  private static Response clientBadRequestError(String message)
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

  public static class ViewDefinitionRequest
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

  /**
   * Response format for {@link #getViewLoadStatus(String, HttpServletRequest)}.
   *
   * The response consists of four lists of hostname:port strings, representing brokers in the cluster.
   *
   * Note that the lists are expected to be sorted by natural string order.
   *
   * fresh: brokers with a view definition that has the same modification time as the coordinator's view definition
   * stale: brokers with an older view than the coordinator
   * newer: brokers with a newer view than the coordinator
   *        (i.e. the view definition changed while the load status API was being processed)
   * unknown: brokers that we were unable to determine view definition version for, e.g. due to network errors
   */
  public static class ViewLoadStatusResponse
  {
    private final List<String> fresh;
    private final List<String> stale;
    private final List<String> newer;
    private final List<String> absent;
    private final List<String> unknown;

    @JsonCreator
    public ViewLoadStatusResponse(
        @JsonProperty("fresh") List<String> fresh,
        @JsonProperty("stale") List<String> stale,
        @JsonProperty("newer") List<String> newer,
        @JsonProperty("absent") List<String> absent,
        @JsonProperty("unknown") List<String> unknown
    )
    {
      this.fresh = fresh;
      this.stale = stale;
      this.newer = newer;
      this.absent = absent;
      this.unknown = unknown;
    }

    @JsonProperty
    public List<String> getFresh()
    {
      return fresh;
    }

    @JsonProperty
    public List<String> getStale()
    {
      return stale;
    }

    @JsonProperty
    public List<String> getNewer()
    {
      return newer;
    }

    @JsonProperty
    public List<String> getAbsent()
    {
      return absent;
    }

    @JsonProperty
    public List<String> getUnknown()
    {
      return unknown;
    }

    /**
     * Note that ordering of the lists matters, the lists are expected to be sorted by natural order.
     */
    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ViewLoadStatusResponse that = (ViewLoadStatusResponse) o;
      return Objects.equals(getFresh(), that.getFresh()) &&
             Objects.equals(getStale(), that.getStale()) &&
             Objects.equals(getNewer(), that.getNewer()) &&
             Objects.equals(getAbsent(), that.getAbsent()) &&
             Objects.equals(getUnknown(), that.getUnknown());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getFresh(), getStale(), getNewer(), getAbsent(), getUnknown());
    }
  }

  private ViewLoadStatusResponse getViewLoadStatusForCluster(ImplyViewDefinition targetView)
  {
    DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeRole(NodeRole.BROKER);
    Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
    List<DiscoveryDruidNode> nodeList = ImmutableList.copyOf(nodes);
    if (nodeList.isEmpty()) {
      throw new RE("Could not discover any brokers when fetching view load status.");
    }

    List<ListenableFuture<StatusResponseHolder>> responseFutures = sendViewStatusRequests(
        targetView.getViewName(),
        nodeList
    );

    try {
      List<StatusResponseHolder> responses = Futures.successfulAsList(responseFutures)
                                                    .get(
                                                        viewStateManagementConfig.getCacheNotificationTimeout(),
                                                        TimeUnit.MILLISECONDS
                                                    );

      List<String> fresh = new ArrayList<>();
      List<String> stale = new ArrayList<>();
      List<String> newer = new ArrayList<>();
      List<String> absent = new ArrayList<>();
      List<String> unknown = new ArrayList<>();

      for (int i = 0; i < responses.size(); i++) {
        StatusResponseHolder response = responses.get(i);
        DiscoveryDruidNode broker = nodeList.get(i);
        String brokerHostAndPort = broker.getDruidNode().getHostAndPortToUse();

        if (response == null) {
          LOG.error("Got null future response from load status request.");
          unknown.add(brokerHostAndPort);
        } else if (HttpResponseStatus.NOT_FOUND.equals(response.getStatus())) {
          absent.add(brokerHostAndPort);
        } else if (!HttpResponseStatus.OK.equals(response.getStatus())) {
          LOG.error("Got error status [%s], content [%s]", response.getStatus(), response.getContent());
          unknown.add(brokerHostAndPort);
        } else {
          LOG.debug("Got status [%s]", response.getStatus());
          try {
            ImplyViewDefinition viewDefinitionFromBroker = jsonMapper.readValue(
                response.getContent(),
                ImplyViewDefinition.class
            );
            int modificationTimeCompareResult = targetView.getLastModified()
                                                          .compareTo(viewDefinitionFromBroker.getLastModified());
            if (modificationTimeCompareResult > 0) {
              stale.add(brokerHostAndPort);
            } else if (modificationTimeCompareResult == 0) {
              fresh.add(brokerHostAndPort);
            } else {
              // view was updated while we were still processing this API
              newer.add(brokerHostAndPort);
            }
          }
          catch (JsonProcessingException e) {
            LOG.error(
                e,
                "Could not deserialize view state from broker[%s], content[%s]",
                brokerHostAndPort,
                response.getContent()
            );
            unknown.add(brokerHostAndPort);
          }
        }
      }

      fresh.sort(Comparator.naturalOrder());
      stale.sort(Comparator.naturalOrder());
      newer.sort(Comparator.naturalOrder());
      absent.sort(Comparator.naturalOrder());
      unknown.sort(Comparator.naturalOrder());

      return new ViewLoadStatusResponse(
          fresh,
          stale,
          newer,
          absent,
          unknown
      );
    }
    catch (Exception e) {
      LOG.error(e, "Failed to get responses for view load status.");
      throw new RE(e);
    }
  }

  private static class AlwaysFailListenableFuture<V> implements ListenableFuture<V>
  {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return false;
    }

    @Override
    public boolean isCancelled()
    {
      return false;
    }

    @Override
    public boolean isDone()
    {
      return true;
    }

    @Override
    public V get() throws ExecutionException
    {
      throw new ExecutionException(new RuntimeException("Could not create view load status future successfully"));
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws ExecutionException
    {
      throw new ExecutionException(new RuntimeException("Could not create view load status future successfully"));
    }

    @Override
    public void addListener(Runnable listener, Executor executor)
    {
      throw new RejectedExecutionException(new RuntimeException("Could not create view load status future successfully"));
    }
  }

  private List<ListenableFuture<StatusResponseHolder>> sendViewStatusRequests(
      String viewName,
      List<DiscoveryDruidNode> nodeList
  )
  {
    List<ListenableFuture<StatusResponseHolder>> futures = new ArrayList<>();
    for (DiscoveryDruidNode node : nodeList) {
      URL listenerURL = CommonStateNotifier.getListenerURL(
          node.getDruidNode(),
          StringUtils.format("/druid/v1/views/listen/%s", viewName)
      );
      if (listenerURL == null) {
        LOG.error("Got null listener URL when sending view status request, node[%s]", node.getDruidNode());
        futures.add(new AlwaysFailListenableFuture<>());
        continue;
      }

      try {
        // best effort
        Request req = new Request(
            HttpMethod.GET,
            listenerURL
        );

        ListenableFuture<StatusResponseHolder> future = httpClient.go(
            req,
            StatusResponseHandler.getInstance(),
            Duration.millis(viewStateManagementConfig.getCacheNotificationTimeout())
        );
        futures.add(future);
      }
      catch (Exception e) {
        LOG.error(e, "Got exception while sending view status request, node[%s]", node.getDruidNode());
        futures.add(new AlwaysFailListenableFuture<>());
      }
    }

    return futures;
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
    SqlQuery query = new SqlQuery("EXPLAIN PLAN FOR " + viewSql, null, false, false, false, null, null);
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
