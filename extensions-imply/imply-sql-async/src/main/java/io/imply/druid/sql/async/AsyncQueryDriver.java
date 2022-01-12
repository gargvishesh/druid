/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

/**
 * Interface to an async query "engine". Allows the Async API
 * to work with a variety of async solutions.
 */
public interface AsyncQueryDriver
{
  String ERROR_KEY = "error";
  String ASYNC_RESULT_KEY = "asyncResultId";

  Response submit(SqlQuery sqlQuery, HttpServletRequest req);
  Response status(String id, HttpServletRequest req);
  Response details(String id, HttpServletRequest req);
  Response results(String id, HttpServletRequest req);
  Response delete(String id, HttpServletRequest req);
}
