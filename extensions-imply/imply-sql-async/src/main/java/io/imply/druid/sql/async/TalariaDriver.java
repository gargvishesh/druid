/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Talaria version of async query execution, with Talaria running
 * within the Overlord/Indexer system. Provides details about
 * task execution in the format provided by the Indexer. Deletion
 * cancels queries, but cannot delete any results saved to cold
 * storage. At present, only the "array" result format is
 * supported, without type or column information.
 * <p>
 * Security is provided via a hybrid approach. On query submit, we use the
 * standard query security path. Talaria runs the query in the indexer, and only
 * the remote Talaria engine has the information about user, data source, etc.
 * For subsequent API calls (status, details, results, cancel),  all we have in
 * this API is the user identity and query ID. To enforce security, we piggy-back
 * on top of the Broker Async engine's metadata store: we add an "async query details"
 * record to the metadata, and use the Broker engine's authorization mechanism.
 * Clunky and slow, but prevents security holes.
 */
public class TalariaDriver extends AbstractAsyncQueryDriver
{
  private static final Logger log = new Logger(TalariaDriver.class);

  public static final String ENGINE_NAME = "Talaria-Indexer";

  public TalariaDriver(AsyncQueryContext context)
  {
    super(context, ENGINE_NAME);
  }

  @SuppressWarnings("resource")
  @Override
  public Response submit(SqlQuery sqlQuery, HttpServletRequest req)
  {
    // This check should never fail or the delegating driver
    // did something wrong.
    if (!isTalaria(sqlQuery)) {
      return genericError(
          Response.Status.INTERNAL_SERVER_ERROR,
          "Internal error",
          "Query does not have the \"talaria\" context flag set",
          null);
    }

    // A Talaria query looks like a regular query, but returns the Task ID
    // as its only output.
    final SqlLifecycle lifecycle = context.sqlLifecycleFactory.factorize();
    final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
    try {
      lifecycle.setParameters(sqlQuery.getParameterList());
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
              "Talaria failed to return a query ID",
               null);
        }
        final SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew(
            id,
            lifecycle.getAuthenticationResult().getIdentity(),
            ResultFormat.ARRAY
        ).toRunning();
        context.metadataManager.addNewQuery(queryDetails);
        return Response
            .status(Response.Status.ACCEPTED)
            .entity(queryDetails.toApiResponse(ENGINE_NAME))
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
      throw (ForbiddenException) context.serverConfig.getErrorResponseTransformStrategy()
                                             .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      SqlPlanningException spe = new SqlPlanningException(SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage());
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    // calcite throws a java.lang.AssertionError which is type error not exception. using throwable will catch all
    catch (Throwable e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);

      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e),
          sqlQueryId
      );
    }
  }

  protected static boolean isTalaria(SqlQuery sqlQuery)
  {
    // Here because this module does not have run-time visibility
    // to the Talaria module.
    // Use same value as ImplyQueryMakerFactory.CTX_TALARIA
    Object value = sqlQuery.getContext().get("talaria");
    if (value == null) {
      return false;
    }
    return Numbers.parseBoolean(value);
  }

  private Response buildNonOkResponse(int status, SanitizableException e, String sqlQueryId)
  {
    // Though transformIfNeeded returns an exception, its purpose is to return
    // a QueryException, which is what SqlAsyncQueryDetailsApiResponse expects.
    QueryException cleaned = (QueryException) context
        .serverConfig
        .getErrorResponseTransformStrategy()
        .transformIfNeeded(e);
    return Response
        .status(status)
        .entity(new SqlAsyncQueryDetailsApiResponse(
            sqlQueryId,
            SqlAsyncQueryDetails.State.UNDETERMINED,
            null,
            0,
            cleaned,
            ENGINE_NAME))
        .build();
  }

  private SqlAsyncQueryDetails updateState(SqlAsyncQueryDetails queryDetails, HttpServletRequest req) throws Exception
  {
    authorizeForQuery(queryDetails, req);
    TaskStatusResponse taskResponse = context.overlordClient.getTaskStatus(queryDetails.getAsyncResultId());
    SqlAsyncQueryDetails.State state;
    switch (taskResponse.getStatus().getStatusCode()) {
      case FAILED:
        state = SqlAsyncQueryDetails.State.FAILED;
        break;
      case RUNNING:
        state = SqlAsyncQueryDetails.State.RUNNING;
        break;
      case SUCCESS:
        state = SqlAsyncQueryDetails.State.COMPLETE;
        break;
      default:
        state = SqlAsyncQueryDetails.State.UNDETERMINED;
        break;
    }
    if (state != queryDetails.getState()) {
      queryDetails = queryDetails.toState(state);

      // No real reason to update the state: we'll never read it
      // directly...
      context.metadataManager.updateQueryDetails(queryDetails);
    }
    return queryDetails;
  }

  @Override
  public Response status(String id, HttpServletRequest req)
  {
    final Optional<SqlAsyncQueryDetails> optQueryDetails = context.metadataManager.getQueryDetails(id);
    if (!optQueryDetails.isPresent()) {
      return notFound(id);
    }
    try {
      SqlAsyncQueryDetails queryDetails = updateState(optQueryDetails.get(), req);
      return Response
          .ok()
          .entity(queryDetails.toApiResponse(ENGINE_NAME))
          .build();
    }
    catch (Exception e) {
      return commErrorResponse(id, e);
    }
  }

  @Override
  public Response details(String id, HttpServletRequest req)
  {
    // Details are engine-dependent. No SqlAsyncQueryDetailsApiResponse
    // wrapper. No update of status.
    final Optional<SqlAsyncQueryDetails> optQueryDetails = context.metadataManager.getQueryDetails(id);
    if (!optQueryDetails.isPresent()) {
      return notFound(id);
    }
    SqlAsyncQueryDetails queryDetails = optQueryDetails.get();
    authorizeForQuery(queryDetails, req);
    try {
      Map<String, Object> report = context.overlordClient.getTaskReport(id);
      removeResults(report);
      return Response
          .ok()
          .entity(report)
          .build();
    }
    catch (Exception e) {
      return commErrorResponse(id, e);
    }
  }

  @Override
  public Response results(String id, HttpServletRequest req)
  {
    final Optional<SqlAsyncQueryDetails> optQueryDetails = context.metadataManager.getQueryDetails(id);
    if (!optQueryDetails.isPresent()) {
      return notFound(id);
    }
    try {
      SqlAsyncQueryDetails queryDetails = updateState(optQueryDetails.get(), req);
      switch (queryDetails.getState()) {
        case COMPLETE:
          break;
        case INITIALIZED:
        case RUNNING:
          return genericError(
              Response.Status.NOT_FOUND,
              "Still running",
              "Query has not yet completed. Poll status until completion.",
              id);
        default:
          return genericError(
              Response.Status.NOT_FOUND,
              "Not found",
              "Query failed. Check details for reason.",
              id);
      }
      Map<String, Object> report = context.overlordClient.getTaskReport(id);
      List<Object> results = getResults(report);
      if (results == null) {
        return genericError(
            Response.Status.NOT_FOUND,
            "Not found",
            "Query produced no results. INSERT query?",
            id);
      }
      return Response
          .ok()
          .entity(results)
          .build();
    }
    catch (Exception e) {
      return commErrorResponse(id, e);
    }
  }

  @Override
  public Response delete(String id, HttpServletRequest req)
  {
    final Optional<SqlAsyncQueryDetails> optQueryDetails = context.metadataManager.getQueryDetails(id);
    if (!optQueryDetails.isPresent()) {
      return notFound(id);
    }
    SqlAsyncQueryDetails queryDetails = optQueryDetails.get();
    authorizeForQuery(queryDetails, req);
    try {
      context.overlordClient.cancelTask(id);
      context.metadataManager.removeQueryDetails(queryDetails);
      return Response
          .status(Response.Status.ACCEPTED)
          .entity(ImmutableMap.of(
              ASYNC_RESULT_KEY, id))
          .build();
    }
    catch (Exception e) {
      return commErrorResponse(id, e);
    }
  }

  private Response commErrorResponse(String id, Exception e)
  {
    // As it turns out, the Overlord returns a text message in place
    // of JSON, which the JSON parser then complains about. Fortunately,
    // the JSON exception includes the original error text which we
    // recover here.
    if (e.getCause() instanceof JsonParseException) {
      String msg = e.getMessage();
      Pattern p = Pattern.compile("\\[Source: \\(String\\)\"([^\"]*)\";");
      Matcher m = p.matcher(msg);
      if (m.find()) {
        return genericError(
            Response.Status.NOT_FOUND,
            "Not found",
            m.group(1),
            id);
      }
    }
    // Else, if we have a clean exception, just use it.
    return genericError(
        Response.Status.INTERNAL_SERVER_ERROR,
        e.getMessage(),
        e.getMessage(),
        id);
  }

  private void removeResults(Map<String, Object> results)
  {
    Map<String, Object> talaria = getMap(results, "talariaResults");
    Map<String, Object> payload = getMap(talaria, "payload");
    if (payload != null) {
      payload.remove("results");
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getMap(Map<String, Object> map, String key)
  {
    if (map == null) {
      return null;
    }
    return (Map<String, Object>) map.get(key);
  }

  /**
   * Builds up an array-with-all-headers response from the various
   * fields in the Talaria results response.
   */
  @SuppressWarnings("unchecked")
  private List<Object> getResults(Map<String, Object> results)
  {
    Map<String, Object> talaria = getMap(results, "talariaResults");
    Map<String, Object> payload = getMap(talaria, "payload");
    if (payload == null) {
      return Collections.emptyList();
    }

    List<Object> data = (List<Object>) payload.get("results");
    final List<String> sqlTypes = (List<String>) payload.get("sqlTypeNames");
    List<Map<String, Object>> sigList = (List<Map<String, Object>>) payload.get("signature");
    int len = 0;
    if (data != null) {
      len = data.size();
    }
    List<Object> rows = new ArrayList<>(len + 3);
    if (sigList != null) {
      List<Object> names = new ArrayList<>(sigList.size());
      List<Object> types = new ArrayList<>(sigList.size());
      for (Map<String, Object> col : sigList) {
        names.add(col.get("name"));
        types.add(col.get("type"));
      }
      rows.add(names);
      rows.add(types);
    }
    if (sqlTypes != null) {
      rows.add(sqlTypes);
    }
    if (data != null) {
      rows.addAll(data);
    }
    return rows;
  }
}
