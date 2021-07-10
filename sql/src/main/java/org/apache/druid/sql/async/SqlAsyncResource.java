/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.async;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
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
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Optional;

@Path("/druid/v2/sql/async/")
public class SqlAsyncResource
{
  private static final Logger log = new Logger(SqlAsyncResource.class);

  private final SqlAsyncQueryPool queryPool;
  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncResultManager resultManager;
  private final SqlLifecycleFactory sqlLifecycleFactory;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;

  @Inject
  public SqlAsyncResource(
      final SqlAsyncQueryPool queryPool,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      final SqlLifecycleFactory sqlLifecycleFactory,
      final AuthorizerMapper authorizerMapper,
      @Json final ObjectMapper jsonMapper
  )
  {
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
    String sqlQueryId = null;

    try {
      sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
      lifecycle.setParameters(sqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      final SqlAsyncQueryDetails queryDetails = queryPool.execute(sqlQuery, lifecycle, remoteAddr);
      return Response.ok(queryDetails).build();
    }
    catch (QueryCapacityExceededException cap) {
      lifecycle.emitLogsAndMetrics(cap, remoteAddr, -1);
      return buildNonOkResponse(sqlQueryId, QueryCapacityExceededException.STATUS_CODE, cap);
    }
    catch (QueryUnsupportedException unsupported) {
      lifecycle.emitLogsAndMetrics(unsupported, remoteAddr, -1);
      return buildNonOkResponse(sqlQueryId, QueryUnsupportedException.STATUS_CODE, unsupported);
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      lifecycle.emitLogsAndMetrics(e, remoteAddr, -1);
      return buildNonOkResponse(sqlQueryId, BadQueryException.STATUS_CODE, e);
    }
    catch (ForbiddenException e) {
      return buildNonOkResponse(sqlQueryId, Response.Status.FORBIDDEN.getStatusCode(), e);
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);
      lifecycle.emitLogsAndMetrics(e, remoteAddr, -1);

      final Exception exceptionToReport;

      if (e instanceof RelOptPlanner.CannotPlanException) {
        exceptionToReport = new ISE("Cannot build plan for query: %s", sqlQuery.getQuery());
      } else {
        exceptionToReport = e;
      }

      return buildNonOkResponse(sqlQueryId, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exceptionToReport);
    }
  }

  @GET
  @Path("/{id}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetStatus(
      @PathParam("id") final String sqlQueryId,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final Optional<SqlAsyncQueryDetails> queryDetails = getQueryDetailsAndAuthorizeRequest(sqlQueryId, req);

    if (queryDetails.isPresent()) {
      return Response.ok(queryDetails.get()).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @GET
  @Path("/{id}/results")
  public Response doGetResults(
      @PathParam("id") final String sqlQueryId,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final Optional<SqlAsyncQueryDetails> queryDetails = getQueryDetailsAndAuthorizeRequest(sqlQueryId, req);

    if (queryDetails.isPresent() && queryDetails.get().getState() == SqlAsyncQueryDetails.State.COMPLETE) {
      final Optional<SqlAsyncResults> results = resultManager.readResults(queryDetails.get().getSqlQueryId());

      if (results.isPresent()) {
        // TODO(gianm): Return proper Content-Type, Content-Disposition, Content-Length
        return Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(OutputStream output) throws IOException, WebApplicationException
                  {
                    ByteStreams.copy(results.get().getInputStream(), output);
                  }
                }
            )
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build();
      }
    }

    return Response.status(Response.Status.NOT_FOUND).build();
  }

  private Optional<SqlAsyncQueryDetails> getQueryDetailsAndAuthorizeRequest(
      final String sqlQueryId,
      final HttpServletRequest req
  ) throws IOException
  {
    AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), authorizerMapper);
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    return metadataManager.getQueryDetails(sqlQueryId)
                          .filter(queryDetails -> authenticationResult.getIdentity()
                                                                      .equals(queryDetails.getIdentity()));
  }

  private Response buildNonOkResponse(String sqlQueryId, int status, Exception e) throws JsonProcessingException
  {
    return Response.status(status)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(jsonMapper.writeValueAsBytes(SqlAsyncQueryDetails.createError(sqlQueryId, null, e)))
                   .build();
  }
}
