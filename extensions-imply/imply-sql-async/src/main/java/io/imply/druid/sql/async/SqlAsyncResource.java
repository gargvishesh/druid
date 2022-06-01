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
import com.google.inject.name.Named;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;
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

/**
 * This implementation uses a "driver" to do the actual work. The first version
 * only worked with a broker. A later version worked with Talaria. The present
 * version is back to working only with the Broker, with Talaria using the
 * /sql/task endpoint. The driver structure is left in place in case we
 * end up with another variation later. See an earlier version for the
 * {@code DeletagingDriver} class that shows how to pick a driver
 * dynamically for other than the submit request.
 */
@Path("/druid/v2/sql/async/")
public class SqlAsyncResource
{
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
      final Clock clock
  )
  {
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
        clock);
    this.driver = new BrokerAsyncQueryDriver(context);
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
