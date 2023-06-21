/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.column.ColumnType;

import java.util.List;
import java.util.Objects;

public class TimeseriesToJSONExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "timeseries_to_json";
  public static final ColumnType TYPE = ColumnType.ofComplex("imply-ts-json");
  private static final ExpressionType OUTPUT_TYPE =
      Objects.requireNonNull(ExpressionType.fromColumnType(TYPE), "type is null");

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);

    Expr arg = args.get(0);

    class TimeseriesToJSONExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {

      public TimeseriesToJSONExpr(Expr arg)
      {
        super(NAME, arg);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object evalValue = arg.eval(bindings).value();
        if (evalValue == null) {
          return ExprEval.ofComplex(
              OUTPUT_TYPE,
              null
          );
        }
        if (!(evalValue instanceof SimpleTimeSeriesContainer)) {
          throw new IAE(
              "Expected a timeseries object, but rather found object of type [%s]",
              evalValue.getClass()
          );
        }

        SimpleTimeSeriesContainer simpleTimeSeriesContainer = (SimpleTimeSeriesContainer) evalValue;
        if (simpleTimeSeriesContainer.isNull()) {
          return ExprEval.ofComplex(
              OUTPUT_TYPE,
              null
          );
        }
        return ExprEval.ofComplex(
            OUTPUT_TYPE,
            simpleTimeSeriesContainer.computeSimple()
        );
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return OUTPUT_TYPE;
      }
    }
    return new TimeseriesToJSONExpr(arg);
  }

  @Override
  public String name()
  {
    return NAME;
  }
}
