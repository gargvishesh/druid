/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

public class AsyncQueryPoolConfig
{
  private static final String MAX_CONCURRENT_QUERIES_KEY = "maxConcurrentQueries";
  private static final String MAX_ASYNC_QUERIES_KEY = "maxAsyncQueries";
  private static final String MAX_QUERIES_TO_QUEUE_KEY = "maxQueriesToQueue";

  private static final int DEFAULT_MAX_CONCURRENT_QUERIES = 10;
  private static final int DEFAULT_MAX_ASYNC_QUERIES = 50;
  private static final int DEFAULT_MAX_QUERIES_TO_QUEUE = 20;

  @JsonProperty
  @Min(1)
  private int maxConcurrentQueries;

  @JsonProperty
  @Min(1)
  private int maxAsyncQueries;

  @JsonProperty
  @Min(1)
  private int maxQueriesToQueue;

  @JsonCreator
  public AsyncQueryPoolConfig(
      @JsonProperty(MAX_CONCURRENT_QUERIES_KEY) @Nullable Integer maxConcurrentQueries,
      @JsonProperty(MAX_ASYNC_QUERIES_KEY) @Nullable Integer maxAsyncQueries,
      @JsonProperty(MAX_QUERIES_TO_QUEUE_KEY) @Nullable Integer maxQueriesToQueue
  )
  {
    this.maxConcurrentQueries = maxConcurrentQueries == null
                                ? DEFAULT_MAX_CONCURRENT_QUERIES
                                : maxConcurrentQueries;
    this.maxAsyncQueries = maxAsyncQueries == null
                           ? DEFAULT_MAX_ASYNC_QUERIES
                           : maxAsyncQueries;
    Preconditions.checkArgument(
        this.maxAsyncQueries >= this.maxConcurrentQueries,
        StringUtils.format(
            "%s [%s] must be greater than or equal to %s [%s]",
            MAX_ASYNC_QUERIES_KEY,
            String.join(".", SqlAsyncModule.BASE_ASYNC_CONFIG_KEY, MAX_ASYNC_QUERIES_KEY),
            MAX_CONCURRENT_QUERIES_KEY,
            String.join(".", SqlAsyncModule.BASE_ASYNC_CONFIG_KEY, MAX_CONCURRENT_QUERIES_KEY)
        )
    );
    this.maxQueriesToQueue = maxQueriesToQueue == null
                             ? DEFAULT_MAX_QUERIES_TO_QUEUE
                             : maxQueriesToQueue;
  }

  public int getMaxConcurrentQueries()
  {
    return maxConcurrentQueries;
  }

  public int getMaxAsyncQueries()
  {
    return maxAsyncQueries;
  }

  public int getMaxQueriesToQueue()
  {
    return maxQueriesToQueue;
  }
}
