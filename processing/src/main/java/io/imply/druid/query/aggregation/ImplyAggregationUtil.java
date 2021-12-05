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
  public static final byte AVG_TIMESERIES_CACHE_ID = (byte) -5;
  public static final byte DELTA_TIMESERIES_CACHE_ID = (byte) -6;
  public static final byte INTERPOLATION_POST_AGG_CACHE_ID = (byte) -7;
  public static final byte TWA_POST_AGG_CACHE_ID = (byte) -8;
}
