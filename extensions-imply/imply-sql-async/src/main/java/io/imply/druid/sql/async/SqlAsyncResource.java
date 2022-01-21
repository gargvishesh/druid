/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Named;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.http.SqlQuery;

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

import java.time.Clock;

@Path("/druid/v2/sql/async/")
public class SqlAsyncResource
{
  private static final Logger LOG = new Logger(SqlAsyncResource.class);

  private final AsyncQueryContext context;
  private final AsyncQueryDriver driver;

  @Inject
  public SqlAsyncResource(
      @Named(SqlAsyncModule.ASYNC_BROKER_ID) final String brokerId,
      final SqlAsyncQueryPool queryPool,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      final SqlLifecycleFactory sqlLifecycleFactory,
      final SqlAsyncLifecycleManager sqlAsyncLifecycleManager,
      final AuthorizerMapper authorizerMapper,
      @Json final ObjectMapper jsonMapper,
      final AsyncQueryConfig asyncQueryReadRefreshConfig,
      final Clock clock,
      final ServerConfig serverConfig,
      final Injector injector
  )
  {
    // The IndexingServiceClient is not bound in the Broker by
    // default in Druid. It will be bound only if the Talaria
    // extension is also loaded. To avoid spurious bindings,
    // we also check that Talaria is loaded.
    IndexingServiceClient overlordClient = null;
    DruidLeaderClient druidLeaderClient = null;
    try {
      if (injector == null) {
        // Testing case.
      } else {
        // TODO: This is a fragile check.
        QueryMakerFactory qmFactory = injector.getInstance(QueryMakerFactory.class);
        if (qmFactory.getClass().getName().equals("io.imply.druid.talaria.sql.ImplyQueryMakerFactory")) {
          overlordClient = injector.getInstance(IndexingServiceClient.class);
          druidLeaderClient = injector.getInstance(
              Key.get(DruidLeaderClient.class, IndexingService.class));
        }
      }
    }
    catch (Exception e) {
      // Ignore: not Talaria.
    }
    if (overlordClient == null) {
      LOG.debug("Broker-based async engine available.");
    } else {
      LOG.debug("Broker-based and Talaria async engines available.");
    }
    // TODO(paul): Better to inject the context. Since the context has
    // all the dependencies, the constructor here becomes quite simple.
    this.context = new AsyncQueryContext(
        brokerId,
        queryPool,
        metadataManager,
        resultManager,
        sqlLifecycleFactory,
        sqlAsyncLifecycleManager,
        authorizerMapper,
        jsonMapper,
        asyncQueryReadRefreshConfig,
        clock,
        serverConfig,
        overlordClient,
        druidLeaderClient);
    if (overlordClient == null) {
      this.driver = new BrokerAsyncQueryDriver(context);
    } else {
      this.driver = new DelegatingDriver(context);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  )
  {
    return driver.submit(sqlQuery, req);
  }

  @GET
  @Path("/{id}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetStatus(
      @PathParam("id") final String asyncResultId,
      @Context final HttpServletRequest req
  )
  {
    return driver.status(asyncResultId, req);
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetDetails(
      @PathParam("id") final String asyncResultId,
      @Context final HttpServletRequest req
  )
  {
    return driver.details(asyncResultId, req);
  }

  @GET
  @Path("/{id}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetResults(
      @PathParam("id") final String asyncResultId,
      @Context final HttpServletRequest req
  )
  {
    return driver.results(asyncResultId, req);
  }

  /**
   * Canceling a query cleans up all records of it, as if it never happened. Queries can be canceled while in any
   * state. Canceling a query that has already completed will remove its results.
   *
   * @param asyncResultId asyncResultId
   * @param req           httpServletRequest
   * @return HTTP 404 if the query ID does not exist,expired or originated by different user. HTTP 202 if the deletion
   * request has been accepted.
   */
  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteQuery(@PathParam("id") final String asyncResultId, @Context final HttpServletRequest req)
  {
    return driver.delete(asyncResultId, req);
  }
}
