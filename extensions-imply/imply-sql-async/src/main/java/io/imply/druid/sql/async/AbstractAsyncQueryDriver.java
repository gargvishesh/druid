/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import java.util.HashMap;
import java.util.Map;

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

  protected Map<String, String> getErrorMap(String asyncResultId, String errorMessage)
  {
    Map<String, String> response = new HashMap<>();
    response.put(ASYNC_RESULT_KEY, asyncResultId);
    response.put(ERROR_KEY, errorMessage);
    return response;
  }
}
