/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IRROverTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "irr";
  public static final ColumnType IRR_DEBUG_TYPE = ColumnType.ofComplex("ts-irr-debug");
  private final boolean debug;
  private Map<String, Object> state;

  public IRROverTimeseriesExprMacro()
  {
    this(false);
  }

  protected IRROverTimeseriesExprMacro(boolean debug)
  {
    this.debug = debug;
    if (!debug) {
      state = null;
    }
  }

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 4, 5);

    Expr arg = args.get(0);
    Expr startInvestmentArg = args.get(1);
    Expr endInvestmentArg = args.get(2);
    Interval window = Intervals.of((String) TimeseriesExprUtil.expectLiteral(args.get(3), name(), 4));
    Double initEstimate = 0.1D;
    if (args.size() == 5) {
      initEstimate = (Double) TimeseriesExprUtil.expectLiteral(args.get(4), name(), 5);
    }

    Double finalInitEstimate = initEstimate;
    class IRROverTimeseriesExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {

      public IRROverTimeseriesExpr(List<Expr> args)
      {
        super(IRROverTimeseriesExprMacro.this.name(), args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object evalValue = arg.eval(bindings).value();
        if (evalValue == null) {
          return debug ?
                 ExprEval.ofComplex(Objects.requireNonNull(ExpressionType.fromColumnType(IRR_DEBUG_TYPE)), null) :
                 ExprEval.ofDouble(null);
        }
        if (!(evalValue instanceof SimpleTimeSeriesContainer)) {
          throw new IAE(
              "Expected a timeseries object, but rather found object of type [%s]",
              evalValue.getClass()
          );
        }

        SimpleTimeSeriesContainer simpleTimeSeriesContainer = (SimpleTimeSeriesContainer) evalValue;
        if (simpleTimeSeriesContainer.isNull()) {
          return debug ?
                 ExprEval.ofComplex(Objects.requireNonNull(ExpressionType.fromColumnType(IRR_DEBUG_TYPE)), null) :
                 ExprEval.ofDouble(null);
        }
        double irr = calculateIRR(
            simpleTimeSeriesContainer.computeSimple(),
            window,
            startInvestmentArg.eval(bindings).asDouble(),
            endInvestmentArg.eval(bindings).asDouble(),
            finalInitEstimate,
            1e-9,
            100
        );
        return debug ?
               ExprEval.ofComplex(Objects.requireNonNull(ExpressionType.fromColumnType(IRR_DEBUG_TYPE)), state) :
               ExprEval.ofDouble(irr);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return debug ? ExpressionType.fromColumnType(IRR_DEBUG_TYPE) : ExpressionType.DOUBLE;
      }
    }
    return new IRROverTimeseriesExpr(args);
  }

  @Override
  public String name()
  {
    return NAME;
  }

  private double getNPV(
      SimpleTimeSeries cashFlows,
      double startInvestment,
      double endInvestment,
      double rate,
      Interval window
  )
  {
    double totalNPVForCashFlow = 0;
    for (int i = 0; i < cashFlows.size(); i++) {
      double timeRatio =
          (double) (cashFlows.getTimestamps().getLong(i) - window.getStartMillis()) / window.toDurationMillis();
      double npvPerCashFlow = cashFlows.getDataPoints().getDouble(i) / Math.pow(1 + rate, timeRatio);
      totalNPVForCashFlow += npvPerCashFlow;
    }
    return startInvestment - (endInvestment / (1 + rate)) + totalNPVForCashFlow;
  }

  private double getNPVDerivative(SimpleTimeSeries cashFlows, double endInvestment, double rate, Interval window)
  {
    double totalNPVDerForCashFlow = 0;
    for (int i = 0; i < cashFlows.size(); i++) {
      double timeRatio =
          (double) (cashFlows.getTimestamps().getLong(i) - window.getStartMillis()) / window.toDurationMillis();
      double npvDerPerCashFlow =
          -1 * timeRatio * cashFlows.getDataPoints().getDouble(i) * StrictMath.pow(1 + rate, -1 * timeRatio - 1);
      totalNPVDerForCashFlow += npvDerPerCashFlow;
    }
    return (endInvestment * StrictMath.pow(1 + rate, -2)) + totalNPVDerForCashFlow;
  }

  public double calculateIRR(
      SimpleTimeSeries cashFlows,
      Interval window,
      double startInvestment,
      double endInvestment,
      double estimate,
      double epsilon,
      int maxIterations
  )
  {
    if (debug) {
      state = ImmutableMap.<String, Object>builder()
              .put("cashFlows", cashFlows.toString())
              .put("startValue", Double.toString(startInvestment))
              .put("endValue", Double.toString(endInvestment))
              .put("startEstimate", Double.toString(estimate))
              .put("window", window)
              .put("iterations", new ArrayList<>())
              .build();
    }
    for (int i = 0; i < maxIterations; i++) {
      double npv = getNPV(cashFlows, startInvestment, endInvestment, estimate, window);
      double npvDerivative = getNPVDerivative(cashFlows, endInvestment, estimate, window);

      double newEstimate = estimate - npv / npvDerivative;
      if (debug) {
        ((ArrayList<Object>) state.get("iterations")).add(
            ImmutableMap.of(
                "iteration", Integer.toString(i + 1),
                "npv", Double.toString(npv),
                "npvDerivative", Double.toString(npvDerivative),
                "estimate", Double.toString(newEstimate)
            )
        );
      }
      if (Double.isNaN(newEstimate) || Double.isInfinite(newEstimate) || (Math.abs(newEstimate - estimate) <= epsilon)) {
        estimate = newEstimate;
        break;
      }
      estimate = newEstimate;
    }
    return estimate;
  }
}
