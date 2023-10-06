/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.postprocessors.InterpolatorTimeSeriesFn;
import io.imply.druid.timeseries.interpolation.Interpolator;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.joda.time.Period;

import java.util.List;
import java.util.Objects;

public abstract class InterpolationTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  private final String name;
  private final Interpolator interpolator;
  private final boolean keepBoundariesOnly;
  private static final ExpressionType OUTPUT_TYPE =
      Objects.requireNonNull(ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE), "type is null");

  public InterpolationTimeseriesExprMacro(String name, Interpolator interpolator, boolean keepBoundariesOnly)
  {
    this.name = name;
    this.interpolator = interpolator;
    this.keepBoundariesOnly = keepBoundariesOnly;
  }

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    Expr arg = args.get(0);
    long bucketMillis = new Period(TimeseriesExprUtil.expectLiteral(args.get(1), name, 2)).toStandardDuration().getMillis();

    class InterpolationTimeseriesExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final InterpolatorTimeSeriesFn interpolatorTimeSeriesFn;

      public InterpolationTimeseriesExpr(List<Expr> args)
      {
        super(InterpolationTimeseriesExprMacro.this.name, args);
        this.interpolatorTimeSeriesFn = new InterpolatorTimeSeriesFn(
            bucketMillis,
            interpolator,
            keepBoundariesOnly
        );
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
        SimpleTimeSeries simpleTimeSeries = simpleTimeSeriesContainer.computeSimple();
        return ExprEval.ofComplex(
            OUTPUT_TYPE,
            interpolatorTimeSeriesFn.compute(simpleTimeSeries, simpleTimeSeries.getMaxEntries())
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
    return new InterpolationTimeseriesExpr(args);
  }

  @Override
  public String name()
  {
    return name;
  }

  public static class LinearInterpolationTimeseriesExprMacro extends InterpolationTimeseriesExprMacro
  {
    public LinearInterpolationTimeseriesExprMacro()
    {
      super("linear_interpolation", Interpolator.LINEAR, false);
    }
  }

  public static class LinearInterpolationTimeseriesWithBoundariesExprMacro extends InterpolationTimeseriesExprMacro
  {
    public LinearInterpolationTimeseriesWithBoundariesExprMacro()
    {
      super("linear_boundary", Interpolator.LINEAR, true);
    }
  }

  public static class BackfillInterpolationTimeseriesExprMacro extends InterpolationTimeseriesExprMacro
  {
    public BackfillInterpolationTimeseriesExprMacro()
    {
      super("backfill_interpolation", Interpolator.BACKFILL, false);
    }
  }

  public static class BackfillInterpolationTimeseriesWithBoundariesExprMacro extends InterpolationTimeseriesExprMacro
  {
    public BackfillInterpolationTimeseriesWithBoundariesExprMacro()
    {
      super("backfill_boundary", Interpolator.BACKFILL, true);
    }
  }

  public static class PaddingInterpolationTimeseriesExprMacro extends InterpolationTimeseriesExprMacro
  {
    public PaddingInterpolationTimeseriesExprMacro()
    {
      super("padding_interpolation", Interpolator.PADDING, false);
    }
  }

  public static class PaddingInterpolationTimeseriesWithBoundariesExprMacro extends InterpolationTimeseriesExprMacro
  {
    public PaddingInterpolationTimeseriesWithBoundariesExprMacro()
    {
      super("padded_boundary", Interpolator.PADDING, true);
    }
  }

  public static List<InterpolationTimeseriesExprMacro> getMacros()
  {
    return ImmutableList.of(
        new LinearInterpolationTimeseriesExprMacro(),
        new LinearInterpolationTimeseriesWithBoundariesExprMacro(),
        new PaddingInterpolationTimeseriesExprMacro(),
        new PaddingInterpolationTimeseriesWithBoundariesExprMacro(),
        new BackfillInterpolationTimeseriesExprMacro(),
        new BackfillInterpolationTimeseriesWithBoundariesExprMacro()
    );
  }
}
