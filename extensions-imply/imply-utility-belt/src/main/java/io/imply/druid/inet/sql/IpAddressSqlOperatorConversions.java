/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.sql;

import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.expression.IpAddressExpressions;
import io.imply.druid.inet.segment.virtual.IpAddressFormatVirtualColumn;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;

public class IpAddressSqlOperatorConversions
{
  public static class ParseOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(IpAddressExpressions.ParseExprMacro.NAME))
        .operandTypes(SqlTypeFamily.ANY)
        .returnTypeInference(
            ReturnTypes.explicit(
                new RowSignatures.ComplexSqlType(
                    SqlTypeName.OTHER,
                    IpAddressModule.TYPE,
                    true
                )
            )
        )
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
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
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          operands -> DruidExpression.fromExpression(
              DruidExpression.functionCall(IpAddressExpressions.ParseExprMacro.NAME, operands)
          )
      );
    }
  }

  public static class TryParseOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(IpAddressExpressions.TryParseExprMacro.NAME))
        .operandTypes(SqlTypeFamily.ANY)
        .returnTypeInference(
            ReturnTypes.explicit(
                new RowSignatures.ComplexSqlType(
                    SqlTypeName.OTHER,
                    IpAddressModule.TYPE,
                    true
                )
            )
        )
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
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
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          operands -> DruidExpression.fromExpression(
              DruidExpression.functionCall(IpAddressExpressions.TryParseExprMacro.NAME, operands)
          )
      );
    }
  }

  public static class StringifyOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(IpAddressExpressions.StringifyExprMacro.NAME))
        .operandTypeChecker(
            OperandTypes.or(
                OperandTypes.family(SqlTypeFamily.ANY),
                OperandTypes.sequence(
                    "(expr,compact)",
                    OperandTypes.family(SqlTypeFamily.ANY),
                    OperandTypes.or(
                        OperandTypes.family(SqlTypeFamily.NUMERIC),
                        OperandTypes.family(SqlTypeFamily.BOOLEAN)
                    )
                )
            )
        )
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
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
      final RexCall call = (RexCall) rexNode;

      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands()
      );

      if (druidExpressions == null || (druidExpressions.size() != 1 && druidExpressions.size() != 2)) {
        return null;
      }

      final boolean compact;
      if (druidExpressions.size() == 2) {
        Expr compactExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());
        if (!compactExpr.isLiteral()) {
          return null;
        }
        compact = compactExpr.eval(InputBindings.nilBindings()).asBoolean();
      } else {
        compact = true;
      }
      String fn = DruidExpression.functionCall(IpAddressExpressions.StringifyExprMacro.NAME, druidExpressions);
      if (druidExpressions.get(0).isSimpleExtraction()) {
        return DruidExpression.forVirtualColumn(
            fn,
            (name, outputType, macroTable) -> new IpAddressFormatVirtualColumn(
                name,
                druidExpressions.get(0).getDirectColumn(),
                compact,
                false
            )
        );
      }
      return DruidExpression.fromExpression(fn);
    }
  }

  public static class PrefixOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(IpAddressExpressions.PrefixExprMacro.NAME))
        .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
        .returnTypeInference(
            ReturnTypes.explicit(
                new RowSignatures.ComplexSqlType(
                    SqlTypeName.OTHER,
                    IpAddressModule.TYPE,
                    true
                )
            )
        )
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
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
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          operands -> DruidExpression.fromExpression(
              DruidExpression.functionCall(IpAddressExpressions.PrefixExprMacro.NAME, operands)
          )
      );
    }
  }

  public static class MatchOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(IpAddressExpressions.MatchExprMacro.NAME))
        .operandTypeChecker(
            OperandTypes.sequence("(expr,string)", OperandTypes.ANY, OperandTypes.STRING)
        )
        .returnTypeCascadeNullable(SqlTypeName.BOOLEAN)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
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
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          operands -> DruidExpression.fromExpression(
              DruidExpression.functionCall(IpAddressExpressions.MatchExprMacro.NAME, operands)
          )
      );
    }
  }
}
