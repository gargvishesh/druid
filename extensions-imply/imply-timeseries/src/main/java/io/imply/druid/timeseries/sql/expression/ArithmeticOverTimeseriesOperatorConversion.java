/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql.expression;

import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.sql.TypeUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;

public class ArithmeticOverTimeseriesOperatorConversion implements SqlOperatorConversion
{
  private final String name;

  public ArithmeticOverTimeseriesOperatorConversion(String name)
  {
    this.name = name;
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(name))
        .operandTypeChecker(
            OperandTypes.or(
                OperandTypes.sequence(
                    "'" + name + "'(timeseries, timeseries2)",
                    TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
                    TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE)
                ),
                OperandTypes.sequence(
                    "'" + name + "'(timeseries, timeseries2, shouldNullPoison)",
                    TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
                    TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
                    OperandTypes.LITERAL
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
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  )
  {
    return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, name);
  }

  public static class AddTimeseriesOperatorConversion extends ArithmeticOverTimeseriesOperatorConversion
  {
    public AddTimeseriesOperatorConversion()
    {
      super("add_timeseries");
    }
  }

  public static class SubtractTimeseriesOperatorConversion extends ArithmeticOverTimeseriesOperatorConversion
  {
    public SubtractTimeseriesOperatorConversion()
    {
      super("subtract_timeseries");
    }
  }

  public static class MultiplyTimeseriesOperatorConversion extends ArithmeticOverTimeseriesOperatorConversion
  {
    public MultiplyTimeseriesOperatorConversion()
    {
      super("multiply_timeseries");
    }
  }

  public static class DivideTimeseriesOperatorConversion extends ArithmeticOverTimeseriesOperatorConversion
  {
    public DivideTimeseriesOperatorConversion()
    {
      super("divide_timeseries");
    }
  }

  public static List<SqlOperatorConversion> sqlOperatorConversionList()
  {
    return ImmutableList.of(
      new AddTimeseriesOperatorConversion(),
      new SubtractTimeseriesOperatorConversion(),
      new MultiplyTimeseriesOperatorConversion(),
      new DivideTimeseriesOperatorConversion()
    );
  }
}
