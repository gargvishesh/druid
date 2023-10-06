/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import com.google.common.base.Preconditions;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import java.util.Arrays;
import java.util.List;

public class QuantileOverTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "quantile_over_timeseries";

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    Expr arg = args.get(0);

    final Double percentile = new Double(String.valueOf(TimeseriesExprUtil.expectLiteral(args.get(1), NAME, 2)));
    Preconditions.checkArgument(percentile >= 0 && percentile <= 1);

    class QuantileOverTimeseriesExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {

      public QuantileOverTimeseriesExpr(List<Expr> args)
      {
        super(NAME, args);
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
        String percentileCacheKey = "p" + percentile;
        Object cacheValue = simpleTimeSeriesContainer.getFromCache(percentileCacheKey);
        if (cacheValue != null) {
          if (!(cacheValue instanceof Double)) {
            throw DruidException.defensive(
                "Corrupted Cache : Found a cached value of percentile which is not a double value. "
                + "Instead found a value of type [%s].",
                cacheValue.getClass()
            );
          }
          return ExprEval.ofDouble((Number) cacheValue);
        }
        // clone is a deep copy
        double[] result = simpleTimeSeriesContainer.computeSimple().getDataPoints().getDoubleArray().clone();
        Arrays.sort(result);
        int percentileIndex = (int) Math.min(Math.ceil(percentile * result.length), result.length);
        if (percentileIndex > 0) {
          percentileIndex--; // done to get the array index
        }
        simpleTimeSeriesContainer.addToCache(percentileCacheKey, result[percentileIndex]);
        return ExprEval.ofDouble(result[percentileIndex]);
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

    return new QuantileOverTimeseriesExpr(args);
  }

  @Override
  public String name()
  {
    return NAME;
  }
}
