/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.api;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.talaria.sql.ImplyQueryMakerFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * MSQE API for query execution, with MSQE running
 * within the Overlord system. The client is expected to know that
 * an MSQE query is an OL task, and use OL APIs for status and to
 * fetch the report.
 */
@Path("/druid/v2/sql/task/")
public class SqlTaskResource
{
  private static final Logger LOG = new Logger(SqlTaskResource.class);

  private final SqlLifecycleFactory sqlLifecycleFactory;
  private final QueryMakerFactory queryMakerFactory;
  private final ServerConfig serverConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public SqlTaskResource(
      final SqlLifecycleFactory sqlLifecycleFactory,
      final QueryMakerFactory queryMakerFactory,
      final ServerConfig serverConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.sqlLifecycleFactory = sqlLifecycleFactory;
    this.queryMakerFactory = queryMakerFactory;
    this.serverConfig = serverConfig;
    this.authorizerMapper = authorizerMapper;
  }

  /**
   * API that allows callers to check if this resource is installed without actually issuing a query. If installed,
   * this call returns 200 OK. If not installed, it returns 404 Not Found.
   */
  @GET
  @Path("/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetEnabled(@Context final HttpServletRequest request)
  {
    // All authenticated users are authorized for this API: check an empty resource list.
    final Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        request,
        Collections.emptyList(),
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    if (queryMakerFactory instanceof ImplyQueryMakerFactory) {
      return Response.ok(ImmutableMap.of("enabled", true)).build();
    } else {
      return Response.status(Status.NOT_FOUND).entity(ImmutableMap.of("enabled", false)).build();
    }
  }

  /**
   * Post a query task. A bit convoluted. Use the Broker's query engine to plan
   * the query. The Query Maker ships the query off to Overlord and creates a
   * result set with one row that contains the task ID. Grab that and return it
   * as part of the {@link SqlTaskStatus} status.
   * <p>
   * The user identity is added as a "hidden" context variable to enable the
   * Async-like security model. Would be better to use an impersonation solution
   * when available.
   */
  @SuppressWarnings("resource")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  )
  {
    final Map<String, Object> newContext = new HashMap<>(sqlQuery.getContext());
    newContext.put("multiStageQuery", true);
    final SqlQuery rewrittenQuery = sqlQuery.withQueryContext(newContext);

    // An MSQE query looks like a regular query, but returns the Task ID
    // as its only output.
    final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sqlQueryId = lifecycle.initialize(
        rewrittenQuery.getQuery(),
        new QueryContext(rewrittenQuery.getContext())
    );
    try {
      lifecycle.setParameters(rewrittenQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      lifecycle.plan();
      final Sequence<Object[]> sequence = lifecycle.execute();
      final Yielder<Object[]> yielder0 = Yielders.each(sequence);
      Yielder<Object[]> yielder = yielder0;
      try {
        String id = null;
        while (!yielder.isDone()) {
          final Object[] row = yielder.get();
          if (id == null && row.length > 0) {
            id = (String) row[0];
          }
          yielder = yielder.next(null);
        }
        if (id == null) {
          // Note: no ID to include in error: that is the problem we're
          // reporting.
          return genericError(
              Response.Status.INTERNAL_SERVER_ERROR,
              "Internal error",
              "MSQE failed to return a query ID",
              null
          );
        }
        return Response
            .status(Response.Status.ACCEPTED)
            .entity(new SqlTaskStatus(
                id,
                TaskState.RUNNING,
                null
            ))
            .build();
      }
      finally {
        try {
          yielder.close();
        }
        catch (IOException e) {
          // Ignore
        }
      }
    }
    // Kitchen-sinking the errors since the are all unchecked.
    // Just copied from SqlResource.
    catch (QueryCapacityExceededException cap) {
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap, sqlQueryId);
    }
    catch (QueryUnsupportedException unsupported) {
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported, sqlQueryId);
    }
    catch (QueryTimeoutException timeout) {
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout, sqlQueryId);
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e, sqlQueryId);
    }
    catch (ForbiddenException e) {
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy()
                                             .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      SqlPlanningException spe = new SqlPlanningException(
          SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage()
      );
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    // Calcite throws a java.lang.AssertionError which is type error not exception. using throwable will catch all
    catch (Throwable e) {
      LOG.noStackTrace().warn(e, "Failed to handle query [%s].", sqlQueryId);

      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e),
          sqlQueryId
      );
    }
  }

  private Response buildNonOkResponse(int status, SanitizableException e, String sqlQueryId)
  {
    // Though transformIfNeeded returns an exception, its purpose is to return
    // a QueryException, which is what SqlAsyncQueryDetailsApiResponse expects.
    QueryException cleaned = (QueryException) serverConfig
        .getErrorResponseTransformStrategy()
        .transformIfNeeded(e);
    return Response
        .status(status)
        .entity(new SqlTaskStatus(
            sqlQueryId,
            TaskState.FAILED,
            cleaned
        ))
        .build();
  }

  private Response genericError(Response.Status status, String code, String msg, String id)
  {
    return Response
        .status(status)
        .entity(new SqlTaskStatus(
            id,
            TaskState.FAILED,
            new QueryException("FAILED", msg, null, null)
        ))
        .build();
  }
}
