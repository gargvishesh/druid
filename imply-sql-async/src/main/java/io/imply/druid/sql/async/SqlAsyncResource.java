/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;

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
import java.util.Collections;
import java.util.Optional;

@Path("/druid/v2/sql/async/")
public class SqlAsyncResource
{
  private static final Logger log = new Logger(SqlAsyncResource.class);

  private final String brokerId;
  private final SqlAsyncQueryPool queryPool;
  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncResultManager resultManager;
  private final SqlLifecycleFactory sqlLifecycleFactory;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;

  @Inject
  public SqlAsyncResource(
      @Named(SqlAsyncModule.ASYNC_BROKER_ID) final String brokerId,
      final SqlAsyncQueryPool queryPool,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      final SqlLifecycleFactory sqlLifecycleFactory,
      final AuthorizerMapper authorizerMapper,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.brokerId = brokerId;
    this.queryPool = queryPool;
    this.metadataManager = metadataManager;
    this.resultManager = resultManager;
    this.sqlLifecycleFactory = sqlLifecycleFactory;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String remoteAddr = req.getRemoteAddr();
    final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
    final String asyncResultId = SqlAsyncUtil.createAsyncResultId(brokerId, sqlQueryId);
    final ResultFormat resultFormat = sqlQuery.getResultFormat();

    try {
      lifecycle.setParameters(sqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      final SqlAsyncQueryDetails queryDetails = queryPool.execute(asyncResultId, sqlQuery, lifecycle, remoteAddr);
      return Response.status(Response.Status.ACCEPTED).entity(queryDetails.toApiResponse()).build();
    }
    catch (QueryCapacityExceededException cap) {
      lifecycle.finalizeStateAndEmitLogsAndMetrics(cap, remoteAddr, -1);
      return buildImmediateErrorResponse(
          asyncResultId,
          resultFormat,
          QueryCapacityExceededException.STATUS_CODE,
          cap
      );
    }
    catch (QueryUnsupportedException unsupported) {
      lifecycle.finalizeStateAndEmitLogsAndMetrics(unsupported, remoteAddr, -1);
      return buildImmediateErrorResponse(
          asyncResultId,
          resultFormat,
          QueryUnsupportedException.STATUS_CODE,
          unsupported
      );
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      lifecycle.finalizeStateAndEmitLogsAndMetrics(e, remoteAddr, -1);
      return buildImmediateErrorResponse(
          asyncResultId,
          resultFormat,
          BadQueryException.STATUS_CODE,
          e
      );
    }
    catch (ForbiddenException e) {
      // Let ForbiddenExceptionMapper handle it.
      throw e;
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);
      lifecycle.finalizeStateAndEmitLogsAndMetrics(e, remoteAddr, -1);

      final Exception exceptionToReport;

      if (e instanceof RelOptPlanner.CannotPlanException) {
        exceptionToReport = new ISE("Cannot build plan for query");
      } else {
        exceptionToReport = e;
      }

      return buildImmediateErrorResponse(
          asyncResultId,
          resultFormat,
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          exceptionToReport
      );
    }
  }

  @GET
  @Path("/{id}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetStatus(
      @PathParam("id") final String asyncResultId,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final Optional<SqlAsyncQueryDetails> queryDetails = getQueryDetailsAndAuthorizeRequest(asyncResultId, req);

    if (queryDetails.isPresent()) {
      return Response.ok(queryDetails.get().toApiResponse()).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @GET
  @Path("/{id}/results")
  public Response doGetResults(
      @PathParam("id") final String asyncResultId,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final Optional<SqlAsyncQueryDetails> queryDetails = getQueryDetailsAndAuthorizeRequest(asyncResultId, req);

    if (queryDetails.isPresent() && queryDetails.get().getState() == SqlAsyncQueryDetails.State.COMPLETE) {
      final Optional<SqlAsyncResults> results = resultManager.readResults(queryDetails.get());

      if (results.isPresent()) {
        return Response.ok(results.get())
                       .type(queryDetails.get().getResultFormat().contentType())
                       .header("Content-Disposition", "attachment")
                       .build();
      }
    }

    return Response.status(Response.Status.NOT_FOUND).build();
  }

  private Optional<SqlAsyncQueryDetails> getQueryDetailsAndAuthorizeRequest(
      final String asyncResultId,
      final HttpServletRequest req
  ) throws IOException
  {
    AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), authorizerMapper);
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    return metadataManager.getQueryDetails(asyncResultId)
                          .filter(
                              queryDetails ->
                                  !Strings.isNullOrEmpty(queryDetails.getIdentity())
                                  && queryDetails.getIdentity().equals(authenticationResult.getIdentity())
                          );
  }

  private Response buildImmediateErrorResponse(
      final String asyncResultId,
      final ResultFormat resultFormat,
      final int status,
      final Exception e
  ) throws JsonProcessingException
  {
    final SqlAsyncQueryDetailsApiResponse errorDetails =
        SqlAsyncQueryDetails.createError(asyncResultId, null, resultFormat, e).toApiResponse();

    return Response.status(status)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(jsonMapper.writeValueAsBytes(errorDetails))
                   .build();
  }
}
