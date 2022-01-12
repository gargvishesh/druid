/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import io.imply.druid.query.aggregation.datasketches.virtual.ImplySessionFilteringVirtualColumn;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public class SessionFilterOperatorConversion implements SqlOperatorConversion
{
  private static final String FUNCTION_NAME = "SESSIONIZE";

  @Override
  public SqlOperator calciteOperator()
  {
    return OperatorConversions
        .operatorBuilder(FUNCTION_NAME)
        .operandTypeChecker(OperandTypes.ANY)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .returnTypeInference(ReturnTypes.explicit(SqlTypeName.VARCHAR))
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
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    if (operands.size() != 1 || !operands.get(0).isA(SqlKind.INPUT_REF)) {
      throw new UnsupportedOperationException("Only accepts direct column in sessionize");
    }
    final RexInputRef ref = (RexInputRef) operands.get(0);
    final String columnName = rowSignature.getColumnName(ref.getIndex());

    DruidExpression druidExpression = DruidExpression.forVirtualColumn(
        StringUtils.format("sessionize(%s)", DruidExpression.fromColumn(columnName).getExpression()),
        (name, outputType, macroTable) -> new ImplySessionFilteringVirtualColumn(name, columnName)
    );

    if (plannerContext.getJoinExpressionVirtualColumnRegistry() != null) {
      VirtualColumn vc = plannerContext.getJoinExpressionVirtualColumnRegistry().getOrCreateVirtualColumnForExpression(
          plannerContext,
          druidExpression,
          ColumnType.STRING
      );
      return DruidExpression.fromColumn(vc.getOutputName());
    }

    return druidExpression;
  }
}
