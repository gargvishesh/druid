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
 * Async query driver that uses information in the query or ID to
 * select between async engines.
 */
public class DelegatingDriver implements AsyncQueryDriver
{
  private final AsyncQueryContext context;
  private final BrokerAsyncQueryDriver brokerDriver;
  private final TalariaDriver tarariaIndexerDriver;

  public DelegatingDriver(AsyncQueryContext context)
  {
    this.context = context;
    this.brokerDriver = new BrokerAsyncQueryDriver(context);
    this.tarariaIndexerDriver = new TalariaDriver(context);
  }

  @Override
  public Response submit(SqlQuery sqlQuery, HttpServletRequest req)
  {
    if (context.overlordClient != null && TalariaDriver.isTalaria(sqlQuery)) {
      return tarariaIndexerDriver.submit(sqlQuery, req);
    } else {
      return brokerDriver.submit(sqlQuery, req);
    }
  }

  @Override
  public Response status(String id, HttpServletRequest req)
  {
    if (isTalariaId(id)) {
      return tarariaIndexerDriver.status(id, req);
    } else {
      return brokerDriver.status(id, req);
    }
  }

  @Override
  public Response details(String id, HttpServletRequest req)
  {
    if (isTalariaId(id)) {
      return tarariaIndexerDriver.details(id, req);
    } else {
      return brokerDriver.details(id, req);
    }
  }

  @Override
  public Response results(String id, HttpServletRequest req)
  {
    if (isTalariaId(id)) {
      return tarariaIndexerDriver.results(id, req);
    } else {
      return brokerDriver.results(id, req);
    }
  }

  @Override
  public Response delete(String id, HttpServletRequest req)
  {
    if (isTalariaId(id)) {
      return tarariaIndexerDriver.delete(id, req);
    } else {
      return brokerDriver.delete(id, req);
    }
  }

  private boolean isTalariaId(String id)
  {
    return context.overlordClient != null && id.startsWith("talaria-sql-");
  }
}
