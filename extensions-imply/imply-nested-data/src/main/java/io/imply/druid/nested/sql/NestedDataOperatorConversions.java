/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.sql;

import com.google.common.collect.ImmutableList;
import io.imply.druid.nested.column.NestedDataComplexTypeSerde;
import io.imply.druid.nested.column.PathFinder;
import io.imply.druid.nested.virtual.NestedFieldVirtualColumn;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.AliasedOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;

import javax.annotation.Nullable;
import java.util.List;

public class NestedDataOperatorConversions
{
  public static class GetPathOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = StringUtils.toUpperCase("get_path");
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(FUNCTION_NAME)
        .operandTypeChecker(
            OperandTypes.sequence(
                "(expr,path)",
                OperandTypes.family(SqlTypeFamily.ANY),
                OperandTypes.family(SqlTypeFamily.STRING)
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

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final Expr pathExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different jq syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<PathFinder.PathPartFinder> parts;
      try {
        parts = PathFinder.parseJqPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw new UnsupportedSQLQueryException(
            "Cannot use [%s]: [%s]",
            call.getOperator().getName(),
            iae.getMessage()
        );
      }
      final String normalized = PathFinder.toNormalizedJqPath(parts);

      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            Calcites.getColumnTypeForRelDataType(call.getType()),
            (args) -> "get_path(" + args.get(0).getExpression() + ",'" + normalized + "')",
            ImmutableList.of(
                DruidExpression.ofColumn(NestedDataComplexTypeSerde.TYPE, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                parts,
                normalized
            )
        );
      }
      throw new UnsupportedSQLQueryException(
          "Cannot use [%s] on expression input: [%s]",
          call.getOperator().getName(),
          druidExpressions.get(0).getExpression()
      );
    }
  }

  public static class JsonGetPathAliasOperatorConversion extends AliasedOperatorConversion
  {
    public JsonGetPathAliasOperatorConversion()
    {
      super(new GetPathOperatorConversion(), StringUtils.toUpperCase("json_get_path"));
    }
  }

  public static class JsonPathsOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("JSON_PATHS")
        .operandTypeChecker(OperandTypes.ANY)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .returnTypeNullableArray(SqlTypeName.VARCHAR)
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
          druidExpressions -> DruidExpression.of(
              null,
              DruidExpression.functionCall("list_paths", druidExpressions)
          )
      );
    }
  }

  public static class JsonKeysOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("JSON_KEYS")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(expr,path)",
                OperandTypes.ANY,
                OperandTypes.and(OperandTypes.family(SqlTypeFamily.STRING), OperandTypes.LITERAL)
            )
        )
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .returnTypeNullableArray(SqlTypeName.VARCHAR)
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
          druidExpressions -> DruidExpression.of(
              null,
              DruidExpression.functionCall("list_keys", druidExpressions)
          )
      );
    }
  }
}
