/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import io.imply.druid.query.aggregation.datasketches.tuple.SessionAvgScoreSummaryStatsPostAggregator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

public class SessionAvgScoreSummaryStatsOperatorConversionBase implements SqlOperatorConversion
{

  private final boolean useAverage;
  private final SqlFunction sqlFunction;

  public SessionAvgScoreSummaryStatsOperatorConversionBase(
      String operatorName,
      boolean useAverage
  )
  {
    this.useAverage = useAverage;
    this.sqlFunction = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(operatorName))
        .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.STRING)
        .returnTypeInference(ReturnTypes.explicit(SqlTypeName.DOUBLE))
        .build();
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return sqlFunction;
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
        postAggregatorVisitor,
        true
    );

    if (firstOperand == null) {
      return null;
    }

    if (!operands.get(1).isA(SqlKind.LITERAL)) {
      return null;
    }

    final String statFn = RexLiteral.stringValue(operands.get(1));
    final SessionAvgScoreSummaryStatsPostAggregator.StatType statType;
    try {
      statType = SessionAvgScoreSummaryStatsPostAggregator.StatType.valueOf(statFn.toUpperCase(Locale.ROOT));
    }
    catch (IllegalArgumentException e) {
      throw new IAE(e, "Unknown statType[%s]", statFn);
    }

    return new SessionAvgScoreSummaryStatsPostAggregator(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        firstOperand,
        statType,
        useAverage
    );
  }
}
