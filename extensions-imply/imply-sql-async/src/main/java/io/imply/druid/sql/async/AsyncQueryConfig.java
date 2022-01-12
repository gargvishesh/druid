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
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

public class AsyncQueryConfig
{
  private static final String MAX_CONCURRENT_QUERIES_KEY = "maxConcurrentQueries";
  private static final String MAX_QUERIES_TO_QUEUE_KEY = "maxQueriesToQueue";
  private static final String READ_REFRESH_TIME = "readRefreshTime";

  @JsonProperty
  @Min(1)
  private int maxConcurrentQueries;

  @JsonProperty
  @Min(1)
  private int maxQueriesToQueue;

  @JsonProperty
  private Duration readRefreshTime;

  @JsonCreator
  public AsyncQueryConfig(
      @JsonProperty(MAX_CONCURRENT_QUERIES_KEY) @Nullable Integer maxConcurrentQueries,
      @JsonProperty(MAX_QUERIES_TO_QUEUE_KEY) @Nullable Integer maxQueriesToQueue,
      @JsonProperty(READ_REFRESH_TIME) @Nullable Duration readRefreshTime
  )
  {
    this.maxConcurrentQueries = maxConcurrentQueries == null
                                ? computeDefaultMaxConcurrentQueries()
                                : maxConcurrentQueries;
    this.maxQueriesToQueue = maxQueriesToQueue == null
                             ? computeDefaultMaxQueriesToQueue(this.maxConcurrentQueries)
                             : maxQueriesToQueue;
    this.readRefreshTime = readRefreshTime == null
                                     ? Duration.standardSeconds(10L)
                                     : readRefreshTime;
    Preconditions.checkArgument(
        (this.readRefreshTime.isEqual(Duration.standardSeconds(1L))
         || this.readRefreshTime.isLongerThan(Duration.standardSeconds(1L))),
        "must have a duration greater than or equal to a 1s duration"
    );
  }

  private int computeDefaultMaxConcurrentQueries()
  {
    // assume async queries are usually 10% of total queries.
    // assume 2 hyper-threads per core, this value is 10% of number of physical cores.
    return (int) Math.max(JvmUtils.getRuntimeInfo().getAvailableProcessors() * 0.05, 1);
  }

  private int computeDefaultMaxQueriesToQueue(int maxConcurrentQueries)
  {
    return Math.max(10, maxConcurrentQueries * 3);
  }

  public int getMaxConcurrentQueries()
  {
    return maxConcurrentQueries;
  }

  public int getMaxQueriesToQueue()
  {
    return maxQueriesToQueue;
  }

  public Duration getReadRefreshTime()
  {
    return readRefreshTime;
  }
}
