/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql.expression;

import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.expression.SumOverTimeseriesExprMacro;
import io.imply.druid.timeseries.sql.TypeUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

public class SumOverTimeseriesOperatorConversion implements SqlOperatorConversion
{
  private static final String NAME = SumOverTimeseriesExprMacro.NAME;

  @Override
  public SqlOperator calciteOperator()
  {
    return OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(NAME))
        .operandTypeChecker(TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE))
        .returnTypeInference(ReturnTypes.explicit(SqlTypeName.DOUBLE))
        .build();
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, NAME);
  }
}
