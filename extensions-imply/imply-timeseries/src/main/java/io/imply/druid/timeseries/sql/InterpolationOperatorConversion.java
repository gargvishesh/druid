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
import io.imply.druid.timeseries.interpolation.Interpolator;
import io.imply.druid.timeseries.postaggregators.InterpolationPostAggregator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
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
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

public abstract class InterpolationOperatorConversion implements SqlOperatorConversion
{
  private final String name;
  private final String interpolator;
  private final boolean keepBoundariesOnly;

  public InterpolationOperatorConversion(String name, String interpolator, boolean keepBoundariesOnly)
  {
    this.name = name;
    this.interpolator = interpolator;
    this.keepBoundariesOnly = keepBoundariesOnly;
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
    List<RexNode> operands = ((RexCall) rexNode).getOperands();
    PostAggregator timeseries = OperatorConversions.toPostAggregator(
        plannerContext,
        rowSignature,
        operands.get(0),
        postAggregatorVisitor,
        false
    );

    if (timeseries == null) {
      return null;
    }

    // get the time bucket millis for intervals
    Long timeBucketMillis = new Period(RexLiteral.stringValue(operands.get(1))).toStandardDuration().getMillis();

    return new InterpolationPostAggregator(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        timeseries,
        Interpolator.valueOf(interpolator.toUpperCase(Locale.ROOT)),
        timeBucketMillis,
        keepBoundariesOnly
    );
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  )
  {
    return null;
  }

  public static class LinearInterpolationOperatorConversion extends InterpolationOperatorConversion
  {
    public LinearInterpolationOperatorConversion()
    {
      super("linear_interpolation", "linear", false);
    }
  }

  public static class LinearInterpolationWithOnlyBoundariesOperatorConversion extends InterpolationOperatorConversion
  {
    public LinearInterpolationWithOnlyBoundariesOperatorConversion()
    {
      super("linear_boundary", "linear", true);
    }
  }

  public static class PaddingInterpolationOperatorConversion extends InterpolationOperatorConversion
  {
    public PaddingInterpolationOperatorConversion()
    {
      super("padding_interpolation", "padding", false);
    }
  }

  public static class PaddingInterpolationWithOnlyBoundariesOperatorConversion extends InterpolationOperatorConversion
  {
    public PaddingInterpolationWithOnlyBoundariesOperatorConversion()
    {
      super("padded_boundary", "padding", true);
    }
  }

  public static class BackfillInterpolationOperatorConversion extends InterpolationOperatorConversion
  {
    public BackfillInterpolationOperatorConversion()
    {
      super("backfill_interpolation", "backfill", false);
    }
  }

  public static class BackfillInterpolationWithOnlyBoundariesOperatorConversion extends InterpolationOperatorConversion
  {
    public BackfillInterpolationWithOnlyBoundariesOperatorConversion()
    {
      super("backfill_boundary", "backfill", true);
    }
  }
}
