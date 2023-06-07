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
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.joda.time.Period;

import java.util.List;
import java.util.Objects;

public class DeltaTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "delta_timeseries";

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 1, 2);

    Expr arg = args.get(0);
    long bucketMillis = 1;
    if (args.size() == 2 && args.get(1).isLiteral()) {
      bucketMillis = new Period(args.get(1).getLiteralValue()).toStandardDuration().getMillis();
    }

    long finalBucketMillis = bucketMillis;
    class DeltaTimeseriesExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {

      public DeltaTimeseriesExpr(Expr arg)
      {
        super(NAME, arg);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object evalValue = arg.eval(bindings).value();
        ExpressionType outputType =
            Objects.requireNonNull(ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE));
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
        return ExprEval.ofComplex(
            outputType,
            buildDeltaSeries(simpleTimeSeriesContainer.computeSimple(), finalBucketMillis)
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
    return new DeltaTimeseriesExpr(arg);
  }

  @Override
  public String name()
  {
    return NAME;
  }

  public static SimpleTimeSeries buildDeltaSeries(SimpleTimeSeries simpleTimeSeries, long bucketMillis)
  {
    ImplyLongArrayList simpleTimeSeriesTimestamps = simpleTimeSeries.getTimestamps();
    ImplyDoubleArrayList simpleTimeSeriesDataPoints = simpleTimeSeries.getDataPoints();
    if (simpleTimeSeriesDataPoints.size() == 0) {
      return simpleTimeSeries;
    }
    ImplyLongArrayList deltaSeriesTimestamps = new ImplyLongArrayList();
    ImplyDoubleArrayList deltaSeriesDataPoints = new ImplyDoubleArrayList();
    DurationGranularity durationGranularity = new DurationGranularity(bucketMillis, 0);
    long currBucketStart = durationGranularity.bucketStart(simpleTimeSeriesTimestamps.getLong(0));
    double prevValue = simpleTimeSeriesDataPoints.getDouble(0);
    double runningSum = 0;
    for (int i = 1; i < simpleTimeSeries.size(); i++) {
      double currValue = simpleTimeSeriesDataPoints.getDouble(i);
      if (currValue > prevValue) {
        long currTimestamp = simpleTimeSeriesTimestamps.getLong(i);
        if (durationGranularity.bucketStart(currTimestamp) > currBucketStart) {
          deltaSeriesTimestamps.add(currBucketStart);
          deltaSeriesDataPoints.add(runningSum);
          runningSum = 0;
          currBucketStart = durationGranularity.bucketStart(currTimestamp);
        }
        runningSum += currValue - prevValue;
        prevValue = currValue;
      }
    }
    deltaSeriesTimestamps.add(currBucketStart);
    deltaSeriesDataPoints.add(runningSum);
    return new SimpleTimeSeries(
        deltaSeriesTimestamps,
        deltaSeriesDataPoints,
        simpleTimeSeries.getWindow(),
        simpleTimeSeries.getStart(),
        simpleTimeSeries.getEnd(),
        simpleTimeSeries.getMaxEntries()
    );
  }
}
