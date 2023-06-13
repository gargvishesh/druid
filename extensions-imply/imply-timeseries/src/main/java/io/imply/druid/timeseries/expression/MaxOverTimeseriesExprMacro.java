/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.util.List;

@EverythingIsNonnullByDefault
public class MaxOverTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "max_over_timeseries";

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);

    Expr arg = args.get(0);

    class MaxValueExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {

      public MaxValueExpr(Expr arg)
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
          return ExprEval.ofDouble(null);
        }
        return ExprEval.ofDouble(getMaxValue(simpleTimeSeriesContainer.computeSimple()));
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.DOUBLE;
      }
    }
    return new MaxValueExpr(arg);
  }

  @Override
  public String name()
  {
    return NAME;
  }

  @Nullable
  public static Double getMaxValue(SimpleTimeSeries simpleTimeSeries)
  {
    if (simpleTimeSeries.size() == 0) {
      return null;
    }
    double maxValue = simpleTimeSeries.getDataPoints().getDouble(0);
    for (double candidate : simpleTimeSeries.getDataPoints()) {
      maxValue = Math.max(maxValue, candidate);
    }
    return maxValue;
  }
}
