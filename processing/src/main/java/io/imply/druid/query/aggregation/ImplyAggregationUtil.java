/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation;

public class ImplyAggregationUtil
{
  // Timeseries
  public static final byte SIMPLE_TIMESERIES_CACHE_ID = (byte) -4;
  public static final byte DOWNSAMPLED_SUM_TIMESERIES_CACHE_ID = (byte) -5;

  // Sessionization
  public static final byte SESSION_AVG_SCORE_TO_HISTOGRAM_FILTERING_CACHE_ID = (byte) -9;
  public static final byte SESSION_FILTERING_VIRTUAL_COLUMN_CACHE_ID = (byte) -10;
  public static final byte SESSION_SAMPLE_RATE_CACHE_ID = (byte) -11;

  // Timeseries
  public static final byte SUM_TIMESERIES_CACHE_ID = (byte) -12;

  public static final byte STRING_BASED = Byte.MIN_VALUE;
}
