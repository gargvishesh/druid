/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.api;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexer.TaskState;
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
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.http.SqlQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

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
 * MSQE API for query execution, with MSQE running
 * within the Overlord system. The client is expected to know that
 * an MSQE query is an OL task, and use OL APIs for status and to
 * fetch the report. For convenience, the API has an endpoint to
 * parse the results of an MSQE SELECT statement and format the
 * results in Druid array format. This saves the client having to
 * do its own parsing, though it is welcome to do so.
 * <p>
 * Security is provided in two ways. On query submit, we use the
 * standard query security path. MSQE runs the query in the Overlord, and only
 * the remote MSQE engine has the information about user, data source, etc.
 * For subsequent API calls (status, details, results, cancel),  all we have in
 * this API are the user identity and query ID. To enforce security, we first ask
 * pass the user identity along with the SQL query context. On each subsequent
 * call, we first get the task info from the Overlord, which returns the identity.
 * We verify identity, and only then issue the requested API. This is
 * a bit of extra work, but the best we can do short-term.
 */
@Path("/druid/v2/sql/task/")
public class SqlTaskResource
{
  private static final Logger LOG = new Logger(SqlTaskResource.class);
  public static final String USER_IDENTITY = "__userIdentity__";

  private final SqlLifecycleFactory sqlLifecycleFactory;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;
  private final ServerConfig serverConfig;
  private final IndexingServiceClient overlordClient;
  private final DruidLeaderClient druidLeaderClient;

  @Inject
  public SqlTaskResource(
      final SqlLifecycleFactory sqlLifecycleFactory,
      final AuthorizerMapper authorizerMapper,
      @Json final ObjectMapper jsonMapper,
      final ServerConfig serverConfig,
      final IndexingServiceClient overlordClient,
      @IndexingService final DruidLeaderClient druidLeaderClient
  )
  {
    this.sqlLifecycleFactory = sqlLifecycleFactory;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
    this.serverConfig = serverConfig;
    this.overlordClient = overlordClient;
    this.druidLeaderClient = druidLeaderClient;
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
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  )
  {
    // The lifeycle authorizes, but we need the user identity now.
    final AuthenticationResult authResult = AuthorizationUtils.authenticationResultFromRequest(req);

    final Map<String, Object> newContext = new HashMap<>(sqlQuery.getContext());
    newContext.put(USER_IDENTITY, authResult.getIdentity());
    newContext.put("talaria", true);
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

  /**
   * The payload is encoded using an MSQE class. However, we want to prune the
   * returned map, which we cannot do with the actual reports class. Instead,
   * we want the reports as a generic map. The Overlord API provides no way to
   * get a generic map, so we use the underlying {@code DruidLeaderClient} to
   * send the request. As it turns out, {@code IndexingServiceClient} uses
   * {@code DruidLeaderClient} internally, but has no API to provide access to
   * that client, so this approach is a bit of a hack. Messy.
   * <p>
   * Note, however, that the REST messages that use this hack are temporary:
   * they will be removed if/when the MSQE SELECT query writes results to a
   * location other than reports.
   */
  private Map<String, Object> getUntypedTaskPayload(String taskId)
  {
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.GET,
              StringUtils.format("/druid/indexer/v1/task/%s", StringUtils.urlEncode(taskId))
          )
      );

      // Don't try to deserialize if something went wrong. We'll
      // just use a null response as "it didn't work out.")
      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        return null;
      }
      return jsonMapper.readValue(
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

  /**
   * Authorizes a request by getting the full Overlord report, then
   * working down to the context variable, then matching the magic
   * user ID variable with the id of the actual user. The request
   * is authorized only if the task is an MSQE task, properly
   * structured, and the user ID matches.
   * <p>
   * @return the task info, or {@code null} if not found. If the
   * return value is {@code null}, the caller should return a NOT FOUND
   * status to the REST client.
   */
  protected Map<String, Object> fetchTaskInfoAndAuthorize(String id, HttpServletRequest req)
  {
    Map<String, Object> taskInfo = getUntypedTaskPayload(id);
    if (taskInfo == null) {
      return null;
    }

    // Now, decode the object to find the user identity we stashed away earlier.
    // If we don't find the entry, it means this is not actually a MSQE task,
    // so the we reject the request for that reason alone.
    Map<String, Object> payload = getMap(taskInfo, "payload");
    if (payload == null) {
      throw new ForbiddenException("Query task");
    }
    Map<String, Object> queryContext = getMap(payload, "sqlQueryContext");
    if (queryContext == null) {
      throw new ForbiddenException("Query task");
    }
    Object identityObj = queryContext.get(USER_IDENTITY);
    if (identityObj == null || !(identityObj instanceof String)) {
      throw new ForbiddenException("Query task");
    }

    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    String identity = (String) identityObj;
    if (!identity.equals(authenticationResult.getIdentity())) {
      throw new ForbiddenException("Query task");
    }
    queryContext.remove(USER_IDENTITY);
    AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), authorizerMapper);
    return taskInfo;
  }

  /**
   * Returns the reports for the MSQE task, minus results.
   * <p>
   * Note: this API is temporary and is a workaround for the
   * fact that results are included in the reports. This WILL
   * change, and this API will be removed in the future.
   */
  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetDetails(
      @PathParam("id") final String id,
      @Context final HttpServletRequest req
  )
  {
    try {
      Map<String, Object> taskInfo = fetchTaskInfoAndAuthorize(id, req);
      if (taskInfo == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      Map<String, Object> report = overlordClient.getTaskReport(id);
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

  /**
   * For a batch SELECT only: return the results from the query.
   * <p>
   * Note: this API is temporary and is a workaround for the
   * fact that results are included in the reports. This WILL
   * change, and this API will be removed in the future.
   */
  @GET
  @Path("/{id}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetResults(
      @PathParam("id") final String id,
      @Context final HttpServletRequest req
  )
  {
    try {
      if (fetchTaskInfoAndAuthorize(id, req) == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      Map<String, Object> report = overlordClient.getTaskReport(id);
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

  /**
   * Builds up an array-with-all-headers response from the various
   * fields in the MSQE results response.
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

  @SuppressWarnings("unchecked")
  private Map<String, Object> getMap(Map<String, Object> map, String key)
  {
    return map == null ? null : (Map<String, Object>) map.get(key);
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
