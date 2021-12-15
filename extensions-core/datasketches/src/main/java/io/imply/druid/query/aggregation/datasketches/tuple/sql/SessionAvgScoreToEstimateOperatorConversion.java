/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchToEstimatePostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public class SessionAvgScoreToEstimateOperatorConversion extends DirectOperatorConversion
{
  private static final String FUNCTION_NAME = "SESSION_AVG_SCORE_ESTIMATE";
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
      .operandTypes(SqlTypeFamily.ANY)
      .returnTypeInference(ReturnTypes.DOUBLE)
      .build();

  public SessionAvgScoreToEstimateOperatorConversion()
  {
    super(SQL_FUNCTION, FUNCTION_NAME);
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
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final PostAggregator firstOperand = OperatorConversions.toPostAggregator(
        plannerContext,
        rowSignature,
        operands.get(0),
        postAggregatorVisitor
    );

    if (firstOperand == null) {
      return null;
    }

    return new ArrayOfDoublesSketchToEstimatePostAggregator(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        firstOperand
    );
  }
}
