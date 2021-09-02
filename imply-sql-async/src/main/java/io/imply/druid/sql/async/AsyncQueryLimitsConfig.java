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

public class AsyncQueryLimitsConfig
{
  private static final int DEFAULT_MAX_SIMULTANEOUS_QUERY_LIMIT = 10;
  private static final int DEFAULT_MAX_QUERY_RETENTION_LIMIT = 50;
  private static final int DEFAULT_MAX_QUERY_QUEUE_SIZE_LIMIT = 20;

  @JsonProperty
  private int maxSimultaneousQuery;

  @JsonProperty
  private int maxQueryRetentionCount;

  @JsonProperty
  private int maxQueryQueueSize;

  @JsonCreator
  public AsyncQueryLimitsConfig(
      @JsonProperty("maxSimultaneousQuery") Integer maxSimultaneousQuery,
      @JsonProperty("maxQueryRetentionCount") Integer maxQueryRetentionCount,
      @JsonProperty("maxQueryQueueSize") Integer maxQueryQueueSize
  )
  {
    this.maxSimultaneousQuery = maxSimultaneousQuery == null ?
                                DEFAULT_MAX_SIMULTANEOUS_QUERY_LIMIT :
                                maxSimultaneousQuery;
    this.maxQueryRetentionCount = maxQueryRetentionCount == null ?
                                  DEFAULT_MAX_QUERY_RETENTION_LIMIT :
                                  maxQueryRetentionCount;
    this.maxQueryQueueSize = maxQueryQueueSize == null ?
                             DEFAULT_MAX_QUERY_QUEUE_SIZE_LIMIT :
                             maxQueryQueueSize;
  }

  public int getMaxSimultaneousQuery()
  {
    return maxSimultaneousQuery;
  }

  public int getMaxQueryRetentionCount()
  {
    return maxQueryRetentionCount;
  }

  public int getMaxQueryQueueSize()
  {
    return maxQueryQueueSize;
  }
}
