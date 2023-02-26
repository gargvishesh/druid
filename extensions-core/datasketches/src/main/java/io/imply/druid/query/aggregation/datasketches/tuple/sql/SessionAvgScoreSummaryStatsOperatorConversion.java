/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

public class SessionAvgScoreSummaryStatsOperatorConversion extends SessionAvgScoreSummaryStatsOperatorConversionBase
{
  public SessionAvgScoreSummaryStatsOperatorConversion()
  {
    super("SESSION_AVG_SCORE_STATS", true);
  }
}