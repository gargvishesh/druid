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
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import java.util.List;

@EverythingIsNonnullByDefault
public class TimeseriesSizeExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "timeseries_size";

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);

    Expr arg = args.get(0);

    class TimeseriesSizeExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {

      public TimeseriesSizeExpr(Expr arg)
      {
        super(NAME, arg);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object evalValue = arg.eval(bindings).value();
        if (evalValue == null) {
          return ExprEval.ofDouble(null);
        }
        if (!(evalValue instanceof SimpleTimeSeriesContainer)) {
          throw new IAE(
              "Expected a timeseries object, but rather found object of type [%s]",
              evalValue.getClass()
          );
        }

        SimpleTimeSeriesContainer simpleTimeSeriesContainer = (SimpleTimeSeriesContainer) evalValue;
        if (simpleTimeSeriesContainer.isNull()) {
          return ExprEval.ofLong(null);
        }
        return ExprEval.ofLong(simpleTimeSeriesContainer.size());
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.LONG;
      }
    }
    return new TimeseriesSizeExpr(arg);
  }

  @Override
  public String name()
  {
    return NAME;
  }
}
