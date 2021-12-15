/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import com.google.common.collect.ImmutableList;
import io.imply.druid.query.aggregation.datasketches.tuple.SessionAvgScoreToHistogramFilteringPostAggregator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public class SessionAvgScoreToHistogramFilteringOperatorConversion implements SqlOperatorConversion
{
  private static final String FUNCTION_NAME = "SESSION_AVG_SCORE_HISTOGRAM_FILTERING";
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
      .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)
      .returnTypeInference(ReturnTypes.explicit(SqlTypeName.VARCHAR))
      .build();

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
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

  @Nullable
  @Override
  public PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature querySignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final PostAggregator firstOperand = OperatorConversions.toPostAggregator(
        plannerContext,
        querySignature,
        operands.get(0),
        postAggregatorVisitor
    );

    if (firstOperand == null) {
      return null;
    }

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        querySignature,
        ImmutableList.of(operands.get(1), operands.get(2))
    );

    if (druidExpressions == null || druidExpressions.size() != 2 || druidExpressions.get(0) == null || druidExpressions.get(1) == null) {
      return null;
    }

    final Expr splitPointsExpr = Parser.parse(druidExpressions.get(0).getExpression(), plannerContext.getExprMacroTable());
    final Expr filterBucketExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());

    if (!splitPointsExpr.isLiteral() || !filterBucketExpr.isLiteral()) {
      return null;
    }

    ExprEval<?> exprEval = splitPointsExpr.eval(InputBindings.nilBindings());
    Double[] splitPointsDouble = exprEval.asDoubleArray();
    if (splitPointsDouble == null) {
      return null;
    }
    double[] splitPoints = new double[splitPointsDouble.length];
    for (int i = 0; i < splitPointsDouble.length; i++) {
      splitPoints[i] = splitPointsDouble[i];
    }

    exprEval = filterBucketExpr.eval(InputBindings.nilBindings());
    Long[] filterBucketsLong = exprEval.asLongArray();
    if (filterBucketsLong == null) {
      return null;
    }
    int[] filterBuckets = new int[filterBucketsLong.length];
    for (int i = 0; i < filterBuckets.length; i++) {
      filterBuckets[i] = Math.toIntExact(filterBucketsLong[i]);
    }

    return new SessionAvgScoreToHistogramFilteringPostAggregator(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        firstOperand,
        splitPoints,
        filterBuckets
    );
  }
}
