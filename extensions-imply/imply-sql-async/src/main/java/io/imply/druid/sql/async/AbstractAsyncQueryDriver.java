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
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import java.util.Collections;

/**
 * Common functionality shared between the Broker an Talaria-in-Indexer
 * async engines.
 */
public abstract class AbstractAsyncQueryDriver implements AsyncQueryDriver
{
  protected final AsyncQueryContext context;
  protected final String engine;

  public AbstractAsyncQueryDriver(AsyncQueryContext context, String engine)
  {
    this.context = context;
    this.engine = engine;
  }

  protected void authorizeForQuery(SqlAsyncQueryDetails queryDetails, HttpServletRequest req)
  {
    AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), context.authorizerMapper);
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    if (Strings.isNullOrEmpty(queryDetails.getIdentity())
           || !queryDetails.getIdentity().equals(authenticationResult.getIdentity())) {
      throw new ForbiddenException("Async query");
    }
  }

  protected Response notFound(String id)
  {
    return genericError(
        Response.Status.NOT_FOUND,
        "Not found",
        "No such query ID.",
        id);
  }

  /**
   * Return an error using the SqlAsyncQueryDetailsApiResponse structure.
   */
  protected Response genericError(Response.Status status, String code, String msg, String id)
  {
    SqlAsyncQueryDetailsApiResponse apiResponse = new SqlAsyncQueryDetailsApiResponse(
        id,
        SqlAsyncQueryDetails.State.UNDETERMINED,
        null,
        0,
        new QueryInterruptedException(
            code,
            msg,
            null,
            null),
        engine);
    return Response
        .status(status)
        .entity(apiResponse)
        .build();
  }
}
