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

import javax.annotation.Nullable;
import java.util.List;

public abstract class UnaryTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);

    Expr arg = args.get(0);

    class UnaryTimeseriesExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {

      public UnaryTimeseriesExpr(Expr arg)
      {
        super(UnaryTimeseriesExprMacro.this.getName(), arg);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object evalValue = arg.eval(bindings).value();
        if (evalValue == null) {
          return ExprEval.ofType(getType(), nullResponse());
        }
        if (!(evalValue instanceof SimpleTimeSeriesContainer)) {
          throw new IAE(
              "Expected a timeseries object, but rather found object of type [%s]",
              evalValue.getClass()
          );
        }

        SimpleTimeSeriesContainer simpleTimeSeriesContainer = (SimpleTimeSeriesContainer) evalValue;
        if (simpleTimeSeriesContainer.isNull()) {
          return ExprEval.ofType(getType(), nullResponse());
        }
        return compute(simpleTimeSeriesContainer);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return getType();
      }
    }
    return new UnaryTimeseriesExpr(arg);
  }

  @Override
  public String name()
  {
    return getName();
  }

  public abstract ExpressionType getType();

  public abstract String getName();

  public abstract ExprEval compute(SimpleTimeSeriesContainer simpleTimeSeriesContainer);

  @Nullable
  public Object nullResponse()
  {
    return null;
  }
}
