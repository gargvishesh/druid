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
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * Security is provided in two ways. On query submit, we use the
 * standard query security path. Talaria runs the query in the indexer, and only
 * the remote Talaria engine has the information about user, data source, etc.
 * For subsequent API calls (status, details, results, cancel),  all we have in
 * this API is the user identity and query ID. To enforce security, we first ask
 * pass the user identity along with the SQL query context. On each subsequent
 * call, we first get the task info from the Overlord, which returns the identity.
 * We verify identity, and only then issue the requested API. This is
 * a bit of extra work, but the best we can do short-term.
 * <p>
 * This means that the Talaria path <i>does not</i> use the Broker engine's
 * metadata store. That avoids any issues of keeping the metadata store in
 * sync with the Overlord.
 */
public class TalariaDriver extends AbstractAsyncQueryDriver
{
  private static final Logger log = new Logger(TalariaDriver.class);

  public static final String ENGINE_NAME = "Talaria-Indexer";
  public static final String ASYNC_IDENTITY = "__asyncIdentity__";

  public TalariaDriver(AsyncQueryContext context)
  {
    super(context, ENGINE_NAME);
  }

  @SuppressWarnings("resource")
  @Override
  public Response submit(
      final SqlQuery sqlQuery,
      final HttpServletRequest req
  )
  {
    // This check should never fail or the delegating driver
    // did something wrong.
    if (!isTalaria(sqlQuery)) {
      return genericError(
          Response.Status.INTERNAL_SERVER_ERROR,
          "Internal error",
          "Query does not have the \"talaria\" context flag set",
          null
      );
    }

    // The lifeycle authorizes, but we need the user identity now.
    final AuthenticationResult authResult = AuthorizationUtils.authenticationResultFromRequest(req);
    final Map<String, Object> queryContext = sqlQuery.getContext();

    // We know there must be a context or this wouldn't be a Talaria query.
    final Map<String, Object> newContext = new HashMap<>(queryContext);
    newContext.put(ASYNC_IDENTITY, authResult.getIdentity());
    final SqlQuery rewrittenQuery = sqlQuery.withQueryContext(newContext);

    // A Talaria query looks like a regular query, but returns the Task ID
    // as its only output.
    final SqlLifecycle lifecycle = context.sqlLifecycleFactory.factorize();
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
              "Talaria failed to return a query ID",
              null
          );
        }
        return Response
            .status(Response.Status.ACCEPTED)
            .entity(new SqlAsyncQueryDetailsApiResponse(
                id,
                SqlAsyncQueryDetails.State.RUNNING,
                ResultFormat.ARRAY,
                0,
                null,
                ENGINE_NAME
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
      throw (ForbiddenException) context.serverConfig.getErrorResponseTransformStrategy()
                                                     .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      SqlPlanningException spe = new SqlPlanningException(
          SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage()
      );
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    // calcite throws a java.lang.AssertionError which is type error not exception. using throwable will catch all
    catch (Throwable e) {
      log.noStackTrace().warn(e, "Failed to handle query [%s].", sqlQueryId);

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
            ENGINE_NAME
        ))
        .build();
  }

  /**
   * The payload is encoded using a Talaria class which is not visible here.
   * Using the getTaskPayload() call results in a class which this code cannot
   * understand. So, reading the payload as generic objects, which turns out to
   * be useful in other ways.
   * <p>
   * Hack method to read a task payload when the JSON subtype is not
   * know to the classes in this process. Occurs when an extension
   * writes the payload, but that extension is not visible to the
   * user of this API.
   */
  private Map<String, Object> getUntypedTaskPayload(String taskId)
  {
    try {
      final StringFullResponseHolder responseHolder = context.druidLeaderClient.go(
          context.druidLeaderClient.makeRequest(
              HttpMethod.GET,
              StringUtils.format("/druid/indexer/v1/task/%s", StringUtils.urlEncode(taskId))
          )
      );

      // Don't try to deserialize if something went wrong. We'll
      // just use a null response as "it didn't work out.")
      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        return null;
      }
      return context.jsonMapper.readValue(
          responseHolder.getContent(),
          new TypeReference<Map<String, Object>>()
          {
          }
      );
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected Map<String, Object> authorize(String id, HttpServletRequest req)
  {
    Map<String, Object> taskInfo = getUntypedTaskPayload(id);
    if (taskInfo == null) {
      return null;
    }

    // Now, decode the object to find the user identity we stashed away earlier.
    // If we don't find the entry, it means this is not actually a Talaria task,
    // so the we reject the request for that reason alone.
    Map<String, Object> payload = getMap(taskInfo, "payload");
    if (payload == null) {
      throw new ForbiddenException("Async query");
    }
    Map<String, Object> queryContext = getMap(payload, "sqlQueryContext");
    if (queryContext == null) {
      throw new ForbiddenException("Async query");
    }
    Object identityObj = queryContext.get(ASYNC_IDENTITY);
    if (identityObj == null || !(identityObj instanceof String)) {
      throw new ForbiddenException("Async query");
    }

    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    String identity = (String) identityObj;
    if (!identity.equals(authenticationResult.getIdentity())) {
      throw new ForbiddenException("Async query");
    }
    queryContext.remove(ASYNC_IDENTITY);
    AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), context.authorizerMapper);
    return taskInfo;
  }

  @Override
  public Response status(String id, HttpServletRequest req)
  {
    try {
      if (authorize(id, req) == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      TaskStatusResponse taskResponse = context.overlordClient.getTaskStatus(id);
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
      SqlAsyncQueryDetailsApiResponse asyncResponse = new SqlAsyncQueryDetailsApiResponse(
          id,
          state,
          ResultFormat.ARRAY,
          0,
          null,
          ENGINE_NAME
      );
      return Response
          .ok()
          .entity(asyncResponse)
          .build();
    }
    catch (ForbiddenException e) {
      throw e;
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
    try {
      Map<String, Object> taskInfo = authorize(id, req);
      if (taskInfo == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      Map<String, Object> report = context.overlordClient.getTaskReport(id);
      addTaskAndRemoveResults(report, getMap(taskInfo, "payload"));

      return Response
          .ok()
          .entity(report)
          .build();
    }
    catch (ForbiddenException e) {
      throw e;
    }
    catch (Exception e) {
      return commErrorResponse(id, e);
    }
  }

  @Override
  public Response results(String id, HttpServletRequest req)
  {
    try {
      if (authorize(id, req) == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      Map<String, Object> report = context.overlordClient.getTaskReport(id);
      List<Object> results = getResults(report);
      if (results == null) {
        return genericError(
            Response.Status.NOT_FOUND,
            "Not found",
            "Query produced no results. INSERT query?",
            id
        );
      }
      return Response
          .ok()
          .entity(results)
          .build();
    }
    catch (ForbiddenException e) {
      throw e;
    }
    catch (Exception e) {
      return commErrorResponse(id, e);
    }
  }

  @Override
  public Response delete(String id, HttpServletRequest req)
  {
    try {
      if (authorize(id, req) == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      context.overlordClient.cancelTask(id);
      return Response
          .status(Response.Status.ACCEPTED)
          .entity(ImmutableMap.of(
              ASYNC_RESULT_KEY, id))
          .build();
    }
    catch (ForbiddenException e) {
      throw e;
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
            id
        );
      }
    }
    // Else, if we have a clean exception, just use it.
    return genericError(
        Response.Status.INTERNAL_SERVER_ERROR,
        e.getMessage(),
        e.getMessage(),
        id
    );
  }

  /**
   * Adds {@code taskPaylaod} to {@code report}, and removes the results.
   */
  private void addTaskAndRemoveResults(Map<String, Object> report, Map<String, Object> taskPayload)
  {
    Map<String, Object> talaria = getMap(report, "talaria");
    if (talaria != null) {
      talaria.put("task", taskPayload);
    }

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
    Map<String, Object> talaria = getMap(results, "talaria");
    Map<String, Object> payload = getMap(talaria, "payload");
    Map<String, Object> resultsHolder = getMap(payload, "results");

    if (resultsHolder == null) {
      return Collections.emptyList();
    }

    List<Object> data = (List<Object>) resultsHolder.get("results");
    List<String> sqlTypes = (List<String>) resultsHolder.get("sqlTypeNames");
    List<Map<String, Object>> sigList = (List<Map<String, Object>>) resultsHolder.get("signature");
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

  private Response genericError(Response.Status status, String code, String msg, String id)
  {
    // Note: Async uses a different error format than the rest of Druid: there is
    // no error key and message. Can't change this fact as the non-standard format
    // is already documented.
    return Response
        .status(status)
        .entity(getErrorMap(id, msg))
        .build();
  }
}
