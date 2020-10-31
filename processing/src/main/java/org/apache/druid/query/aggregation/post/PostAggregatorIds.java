/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.post;

import org.apache.druid.annotations.UsedInContribExtensions;

public class PostAggregatorIds
{
  public static final byte ARITHMETIC = 0;
  public static final byte CONSTANT = 1;
  public static final byte DOUBLE_GREATEST = 2;
  public static final byte DOUBLE_LEAST = 3;
  public static final byte EXPRESSION = 4;
  public static final byte FIELD_ACCESS = 5;
  public static final byte JAVA_SCRIPT = 6;
  public static final byte LONG_GREATEST = 7;
  public static final byte LONG_LEAST = 8;
  public static final byte HLL_HYPER_UNIQUE_FINALIZING = 9;
  public static final byte HISTOGRAM_BUCKETS = 10;
  public static final byte HISTOGRAM_CUSTOM_BUCKETS = 11;
  public static final byte HISTOGRAM_EQUAL_BUCKETS = 12;
  public static final byte HISTOGRAM_MAX = 13;
  public static final byte HISTOGRAM_MIN = 14;
  public static final byte HISTOGRAM_QUANTILE = 15;
  public static final byte HISTOGRAM_QUANTILES = 16;
  public static final byte DATA_SKETCHES_SKETCH_ESTIMATE = 17;
  public static final byte DATA_SKETCHES_SKETCH_SET = 18;
  public static final byte VARIANCE_STANDARD_DEVIATION = 19;
  public static final byte FINALIZING_FIELD_ACCESS = 20;
  public static final byte ZTEST = 21;
  public static final byte PVALUE_FROM_ZTEST = 22;
  public static final byte THETA_SKETCH_CONSTANT = 23;
  @UsedInContribExtensions
  public static final byte MOMENTS_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 24;
  @UsedInContribExtensions
  public static final byte MOMENTS_SKETCH_TO_MIN_CACHE_TYPE_ID = 25;
  @UsedInContribExtensions
  public static final byte MOMENTS_SKETCH_TO_MAX_CACHE_TYPE_ID = 26;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_RANK_CACHE_TYPE_ID = 27;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_CDF_CACHE_TYPE_ID = 28;
  public static final byte THETA_SKETCH_TO_STRING = 29;
  @UsedInContribExtensions
  public static final byte TDIGEST_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 30;
  @UsedInContribExtensions
  public static final byte TDIGEST_SKETCH_TO_QUANTILE_CACHE_TYPE_ID = 31;
  public static final byte HLL_SKETCH_TO_ESTIMATE_CACHE_TYPE_ID = 32;
}
