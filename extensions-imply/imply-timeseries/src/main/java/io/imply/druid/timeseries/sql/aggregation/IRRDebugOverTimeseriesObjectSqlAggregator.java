/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql.aggregation;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;

public class IRRDebugOverTimeseriesObjectSqlAggregator extends IRROverTimeseriesObjectSqlAggregator
{
  private static final String NAME = "IRR_DEBUG";
  private static final SqlAggFunction DEBUG_FUNCTION_INSTANCE = new IRROverTimeSeriesSqlAggFunction(NAME, SqlTypeName.OTHER);

  @Override
  public SqlAggFunction calciteFunction()
  {
    return DEBUG_FUNCTION_INSTANCE;
  }

  @Override
  public String getPostAggregatorName()
  {
    return NAME;
  }
}
