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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.AliasedOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.table.RowSignatures;

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
      final List<PathFinder.PathPart> parts;
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
                outputType,
                parts,
                normalized,
                false
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
          druidExpressions -> DruidExpression.ofExpression(
              null,
              DruidExpression.functionCall("json_paths"),
              druidExpressions
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
          druidExpressions -> DruidExpression.ofExpression(
              null,
              DruidExpression.functionCall("list_keys"),
              druidExpressions
          )
      );
    }
  }

  public static class JsonQueryOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = StringUtils.toUpperCase("json_query");
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(FUNCTION_NAME)
        .operandTypeChecker(OperandTypes.family(new SqlTypeFamily[]{SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY}))
        .returnTypeCascadeNullable(
            new RowSignatures.ComplexSqlType(
                SqlTypeName.OTHER,
                NestedDataComplexTypeSerde.TYPE,
                true
            ).getSqlTypeName()
        )
        .functionCategory(SqlFunctionCategory.SYSTEM)
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
          call.getOperands().subList(0, 2)
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
      final List<PathFinder.PathPart> parts;
      try {
        parts = PathFinder.parseJsonPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw new UnsupportedSQLQueryException(
            "Cannot use [%s]: [%s]",
            call.getOperator().getName(),
            iae.getMessage()
        );
      }
      final String normalized = PathFinder.toNormalizedJqPath(parts);
      final String jsonPath = PathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_query(" + args.get(0).getExpression() + ",'" + jsonPath + "')";
      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            NestedDataComplexTypeSerde.TYPE,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(NestedDataComplexTypeSerde.TYPE, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                outputType,
                parts,
                normalized,
                true
            )
        );
      }
      return DruidExpression.ofExpression(NestedDataComplexTypeSerde.TYPE, builder, druidExpressions);
    }
  }

  public static class JsonValueOperatorConversion implements SqlOperatorConversion
  {
    @Override
    public SqlOperator calciteOperator()
    {
      return SqlStdOperatorTable.JSON_VALUE;
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

      // calcite puts a bunch of junk in here so the call looks something like
      // JSON_VALUE(`nested`.`nest`, '$.x', SQLJSONVALUEEMPTYORERRORBEHAVIOR[NULL], NULL, SQLJSONVALUEEMPTYORERRORBEHAVIOR[NULL], NULL, VARCHAR(2000))
      // by the time it gets here
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands().subList(0, 2)
      );

      ColumnType inferredOutputType = ColumnType.STRING;
      if (call.getOperands().size() == 7) {
        ColumnType maybe = Calcites.getColumnTypeForRelDataType(call.getOperands().get(6).getType());
        if (maybe != null && !ColumnType.UNKNOWN_COMPLEX.equals(maybe)) {
          inferredOutputType = maybe;
        }
      }

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final Expr pathExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different jq syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<PathFinder.PathPart> parts;
      try {
        parts = PathFinder.parseJsonPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw new UnsupportedSQLQueryException(
            "Cannot use [%s]: [%s]",
            call.getOperator().getName(),
            iae.getMessage()
        );
      }
      final String normalized = PathFinder.toNormalizedJqPath(parts);
      final String jsonPath = PathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_value(" + args.get(0).getExpression() + ",'" + jsonPath + "')";

      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            inferredOutputType,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(NestedDataComplexTypeSerde.TYPE, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                outputType,
                parts,
                normalized,
                false
            )
        );
      }
      return DruidExpression.ofExpression(ColumnType.STRING, builder, druidExpressions);
    }
  }

  // calcite converts JSON_VALUE to JSON_VALUE_ANY so we have to wire that up too...
  public static class JsonValueAnyOperatorConversion extends AliasedOperatorConversion
  {
    private static final String FUNCTION_NAME = StringUtils.toUpperCase("json_value_any");

    public JsonValueAnyOperatorConversion()
    {
      super(new JsonValueOperatorConversion(), FUNCTION_NAME);
    }

    @Override
    public SqlOperator calciteOperator()
    {
      return SqlStdOperatorTable.JSON_VALUE_ANY;
    }
  }

  public static class JsonObjectOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "json_object";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(FUNCTION_NAME)
        .operandTypeChecker(OperandTypes.variadic(SqlOperandCountRanges.from(1)))
        .operandTypeInference((callBinding, returnType, operandTypes) -> {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          for (int i = 0; i < operandTypes.length; i++) {
            if (i % 2 == 0) {
              operandTypes[i] = typeFactory.createSqlType(SqlTypeName.VARCHAR);
              continue;
            }
            operandTypes[i] = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true);
          }
        })
        .returnTypeCascadeNullable(
            new RowSignatures.ComplexSqlType(
                SqlTypeName.OTHER,
                NestedDataComplexTypeSerde.TYPE,
                true
            ).getSqlTypeName()
        )
        .functionCategory(SqlFunctionCategory.SYSTEM)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
    {
      final DruidExpression.DruidExpressionCreator expressionFunction = druidExpressions ->
          DruidExpression.ofExpression(
              null,
              null,
              DruidExpression.functionCall("struct"),
              druidExpressions
          );

      final RexCall call = (RexCall) rexNode;

      // we ignore the first argument because calcite sets a 'nullBehavior' parameter by the time it gets here
      // that we .. dont care about right now
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands().subList(1, call.getOperands().size())
      );

      if (druidExpressions == null) {
        return null;
      }

      return expressionFunction.create(druidExpressions);
    }
  }

  public static class ToJsonOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "to_json";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
        .operandTypes(SqlTypeFamily.ANY)
        .returnTypeCascadeNullable(
            new RowSignatures.ComplexSqlType(
                SqlTypeName.OTHER,
                NestedDataComplexTypeSerde.TYPE,
                true
            ).getSqlTypeName()
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
          druidExpressions -> DruidExpression.ofExpression(
              NestedDataComplexTypeSerde.TYPE,
              DruidExpression.functionCall("to_json"),
              druidExpressions
          )
      );
    }
  }

  public static class ToJsonStringOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "to_json_string";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
        .operandTypes(SqlTypeFamily.ANY)
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
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          druidExpressions -> DruidExpression.ofExpression(
              NestedDataComplexTypeSerde.TYPE,
              DruidExpression.functionCall("to_json_string"),
              druidExpressions
          )
      );
    }
  }

  public static class ParseJsonOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "parse_json";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
        .operandTypes(SqlTypeFamily.ANY)
        .returnTypeCascadeNullable(
            new RowSignatures.ComplexSqlType(
                SqlTypeName.OTHER,
                NestedDataComplexTypeSerde.TYPE,
                true
            ).getSqlTypeName()
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
          druidExpressions -> DruidExpression.ofExpression(
              NestedDataComplexTypeSerde.TYPE,
              DruidExpression.functionCall("parse_json"),
              druidExpressions
          )
      );
    }
  }
}
