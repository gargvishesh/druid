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
import io.imply.druid.timeseries.expression.DeltaTimeseriesExprMacro;
import io.imply.druid.timeseries.sql.TypeUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.CastedLiteralOperandTypeCheckers;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

public class DeltaTimeseriesOperatorConversion implements SqlOperatorConversion
{

  @Override
  public SqlOperator calciteOperator()
  {
    return OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(DeltaTimeseriesExprMacro.NAME))
        .operandTypeChecker(
            OperandTypes.or(
                TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
                OperandTypes.sequence(
                    StringUtils.format("%s(timeseriesColumn, bucketPeriod)", DeltaTimeseriesExprMacro.NAME),
                    TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
                    CastedLiteralOperandTypeCheckers.LITERAL
                )
            )
        )
        .returnTypeInference(opBinding -> RowSignatures.makeComplexType(
            opBinding.getTypeFactory(),
            BaseTimeSeriesAggregatorFactory.TYPE,
            true
        ))
        .build();
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, DeltaTimeseriesExprMacro.NAME);
  }
}
