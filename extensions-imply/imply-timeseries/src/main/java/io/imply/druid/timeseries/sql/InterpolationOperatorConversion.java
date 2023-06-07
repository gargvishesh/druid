/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql;

import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

public abstract class InterpolationOperatorConversion implements SqlOperatorConversion
{
  private final String name;

  public InterpolationOperatorConversion(String name)
  {
    this.name = name;
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(name))
        .operandTypeChecker(OperandTypes.sequence("'" + name + "'(timeseries, bucketPeriod)",
                                                  TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
                                                  OperandTypes.LITERAL))
        .returnTypeInference(opBinding -> RowSignatures.makeComplexType(
            opBinding.getTypeFactory(),
            BaseTimeSeriesAggregatorFactory.TYPE,
            true
        ))
        .build();
  }

  @Nullable
  @Override
  public PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    return null;
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

  public static class LinearInterpolationOperatorConversion extends InterpolationOperatorConversion
  {
    public LinearInterpolationOperatorConversion()
    {
      super("linear_interpolation");
    }
  }

  public static class LinearInterpolationWithOnlyBoundariesOperatorConversion extends InterpolationOperatorConversion
  {
    public LinearInterpolationWithOnlyBoundariesOperatorConversion()
    {
      super("linear_boundary");
    }
  }

  public static class PaddingInterpolationOperatorConversion extends InterpolationOperatorConversion
  {
    public PaddingInterpolationOperatorConversion()
    {
      super("padding_interpolation");
    }
  }

  public static class PaddingInterpolationWithOnlyBoundariesOperatorConversion extends InterpolationOperatorConversion
  {
    public PaddingInterpolationWithOnlyBoundariesOperatorConversion()
    {
      super("padded_boundary");
    }
  }

  public static class BackfillInterpolationOperatorConversion extends InterpolationOperatorConversion
  {
    public BackfillInterpolationOperatorConversion()
    {
      super("backfill_interpolation");
    }
  }

  public static class BackfillInterpolationWithOnlyBoundariesOperatorConversion extends InterpolationOperatorConversion
  {
    public BackfillInterpolationWithOnlyBoundariesOperatorConversion()
    {
      super("backfill_boundary");
    }
  }
}
