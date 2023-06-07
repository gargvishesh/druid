/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expressions;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.postprocessors.TimeWeightedAvgTimeSeriesFn;
import io.imply.druid.timeseries.interpolation.Interpolator;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.joda.time.Period;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class TimeWeightedAverageTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "time_weighted_average";

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 3);

    Expr arg = args.get(0);
    Interpolator interpolator = Interpolator.fromString(args.get(1).getLiteralValue().toString().toUpperCase(Locale.ROOT));
    long bucketMillis = new Period(args.get(2).getLiteralValue()).toStandardDuration().getMillis();

    class TimeWeightedAverageTimeseriesExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {

      public TimeWeightedAverageTimeseriesExpr(Expr arg)
      {
        super(NAME, arg);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object evalValue = arg.eval(bindings).value();
        ExpressionType outputType =
            Objects.requireNonNull(ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE), "type is null");
        if (evalValue == null) {
          return ExprEval.ofComplex(
              outputType,
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
              outputType,
              null
          );
        }
        SimpleTimeSeries simpleTimeSeries = simpleTimeSeriesContainer.computeSimple();
        return ExprEval.ofComplex(
            outputType,
            new TimeWeightedAvgTimeSeriesFn(bucketMillis, interpolator)
                .compute(simpleTimeSeries, simpleTimeSeries.getMaxEntries())
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
        return ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE);
      }
    }
    return new TimeWeightedAverageTimeseriesExpr(arg);
  }

  @Override
  public String name()
  {
    return NAME;
  }
}
