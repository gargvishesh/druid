/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.async.exception.AsyncQueryDoesNotExistException;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import io.imply.druid.sql.async.result.AsyncQueryResettingFilterInputStream;
import io.imply.druid.sql.async.result.SqlAsyncResults;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * Implementation of the Async query concept that uses a broker-based
 * approach. This approach does not provide details about query execution
 * (instead it returns the same information as for status.) This version
 * can delete query results after execution completes.
 */
public class BrokerAsyncQueryDriver extends AbstractAsyncQueryDriver
{
  private static final Logger log = new Logger(BrokerAsyncQueryDriver.class);
  private static final String FEATURE_NAME = "druidFeature";
  private static final String FEATURE_VALUE = "async-downloads";
  public static final String ENGINE_NAME = "Broker";

  public BrokerAsyncQueryDriver(AsyncQueryContext context)
  {
    super(context, ENGINE_NAME);
  }

  @Override
  public Response submit(
      final SqlQuery sqlQuery,
      final HttpServletRequest req)
  {
    final SqlLifecycle lifecycle = context.sqlLifecycleFactory.factorize();
    final String remoteAddr = req.getRemoteAddr();
    // Update query to add FEATURE_NAME and FEATURE_VALUE in context
    final SqlQuery updatedSqlQuery = sqlQuery.withQueryContext(
        BaseQuery.computeOverriddenContext(
            ImmutableMap.of(FEATURE_NAME, FEATURE_VALUE),
            sqlQuery.getContext()));

    final String sqlQueryId = lifecycle.initialize(
        updatedSqlQuery.getQuery(),
        new QueryContext(updatedSqlQuery.getContext())
    );
    final String asyncResultId = SqlAsyncUtil.createAsyncResultId(context.brokerId, sqlQueryId);
    final ResultFormat resultFormat = sqlQuery.getResultFormat();

    try {
      lifecycle.setParameters(updatedSqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      final SqlAsyncQueryDetails queryDetails = context.queryPool.execute(asyncResultId, updatedSqlQuery, lifecycle, remoteAddr);
      return Response
          .status(Response.Status.ACCEPTED)
          .entity(queryDetails.toApiResponse(ENGINE_NAME))
          .build();
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
      log.warn(e, "Failed to handle query: %s", updatedSqlQuery);
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

  @Override
  public Response status(
      final String asyncResultId,
      final HttpServletRequest req)
  {
    final Optional<SqlAsyncQueryDetails> queryDetails = context.metadataManager.getQueryDetails(asyncResultId);

    if (!queryDetails.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    authorizeForQuery(queryDetails.get(), req);
    return Response.ok(queryDetails.get().toApiResponse(ENGINE_NAME)).build();
  }

  @Override
  public Response details(
      final String asyncResultId,
      final HttpServletRequest req)
  {
    return status(asyncResultId, req);
  }

  @Override
  public Response results(
      final String asyncResultId,
      final HttpServletRequest req)
  {
    final Optional<SqlAsyncQueryDetails> queryDetails = context.metadataManager.getQueryDetails(asyncResultId);

    if (!queryDetails.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    authorizeForQuery(queryDetails.get(), req);

    if (queryDetails.get().getState() != SqlAsyncQueryDetails.State.COMPLETE) {
      return Response
          .status(Response.Status.NOT_FOUND)
          .entity(ImmutableMap.of(
              ERROR_KEY, "Query is not yet complete.",
              ASYNC_RESULT_KEY, asyncResultId))
          .build();
    }

    try {
      // touch query lastupdate time here to ensure it isn't cleaned up while reading it in the beginning
      context.metadataManager.touchQueryLastUpdateTime(asyncResultId);
      final Optional<SqlAsyncResults> results = context.resultManager.readResults(queryDetails.get());
      if (!results.isPresent()) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response
          .ok(new SqlAsyncResults(
               new AsyncQueryResettingFilterInputStream(
                   results.get().getInputStream(),
                   () -> {
                     try {
                       context.metadataManager.touchQueryLastUpdateTime(asyncResultId);
                     }
                     catch (AsyncQueryDoesNotExistException e) {
                       log.error(
                           "Unable to touch last update time for asyncResultId %s because that ID is not found in the metadata store.",
                           asyncResultId
                       );
                     }
                   },
                   context.asyncQueryReadRefreshConfig.getReadRefreshTime().getMillis(),
                   context.clock
               ),
               results.get().getSize()
           ))
           .type(queryDetails.get().getResultFormat().contentType())
           .header("Content-Disposition", "attachment")
           .build();
    }
    catch (AsyncQueryDoesNotExistException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    catch (IOException e) {
      return Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(ImmutableMap.of(ERROR_KEY, e.getMessage()))
          .build();
    }
  }

  @Override
  public Response delete(
      String asyncResultId,
      HttpServletRequest req)
  {
    log.debug("Received delete request for async query [%s]", asyncResultId);
    Optional<SqlAsyncQueryDetails> queryDetailsOptional = Optional.empty();
    try {
      // We are doing snapshotting here of queryDetails. It can be possible that the queryDetails is changed in
      // the back ground, checkout {@link SqlAsyncQueryPool#execute}, and moves to completed state. In such a case the
      // result set will not be deleted. The job of cleaning such results happen in the coordiantor duty
      // {@link KillAsyncQueryResultWithoutMetadata}.
      queryDetailsOptional = context.metadataManager.getQueryDetails(asyncResultId);
      if (!queryDetailsOptional.isPresent()) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }

      SqlAsyncQueryDetails details = queryDetailsOptional.get();
      authorizeForQuery(details, req);
      // Clean up no matter what.
      // Reduce, not remove, the chances of incorrect output in case cancel with same asyncResultId is called
      // multiple times. Operations below are no-op in case called twice.
      // TODO: Add concurrency controls per asyncResultId so that parallel invocations of delete give correct results
      context.metadataManager.removeQueryDetails(details);
      if (details.getState().equals(SqlAsyncQueryDetails.State.INITIALIZED) || details.getState().equals(
          SqlAsyncQueryDetails.State.RUNNING)) {
        // if running or about to be run
        context.sqlAsyncLifecycleManager.cancel(asyncResultId);
        context.sqlAsyncLifecycleManager.remove(asyncResultId);
      } else if (details.getState().equals(SqlAsyncQueryDetails.State.COMPLETE)) {
        // if completed remove output
        context.resultManager.deleteResults(asyncResultId);
      } else if (details.getState().equals(SqlAsyncQueryDetails.State.FAILED)) {
        // removing state if failed as a safety check
        context.sqlAsyncLifecycleManager.remove(asyncResultId);
      }
      // response for all states of SqlAsyncQueryDetails
      return Response.status(Response.Status.ACCEPTED).build();
    }
    catch (ForbiddenException e) {
      throw e;
    }
    catch (Exception e) {
      log.error(
          e,
          "Unable to clean query %s",
          asyncResultId + (queryDetailsOptional.isPresent()
                           ? queryDetailsOptional.get().toApiResponse(ENGINE_NAME).toString()
                           : "")
      );

      return Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(getErrorMap(asyncResultId, e.getMessage()))
          .build();
    }
  }

  private Response buildImmediateErrorResponse(
      final String asyncResultId,
      final ResultFormat resultFormat,
      final int status,
      final Exception e
  )
  {
    final SqlAsyncQueryDetailsApiResponse errorDetails =
        SqlAsyncQueryDetails.createError(asyncResultId, null, resultFormat, e).toApiResponse(ENGINE_NAME);

    return Response.status(status)
                   .entity(errorDetails)
                   .build();
  }

  private void authorizeForQuery(SqlAsyncQueryDetails queryDetails, HttpServletRequest req)
  {
    AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), context.authorizerMapper);
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    if (Strings.isNullOrEmpty(queryDetails.getIdentity())
           || !queryDetails.getIdentity().equals(authenticationResult.getIdentity())) {
      throw new ForbiddenException("Async query");
    }
  }
}
