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
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
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
  private static final ExpressionType OUTPUT_TYPE =
      Objects.requireNonNull(ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE), "type is null");

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 1, 2);

    Expr arg = args.get(0);

    final Period bucketPeriod;
    if (args.size() == 2) {
      bucketPeriod = new Period(TimeseriesExprUtil.expectLiteral(args.get(1), NAME, 2));
    } else {
      bucketPeriod = Period.millis(1);
    }

    long finalBucketMillis = bucketPeriod.toStandardDuration().getMillis();
    final DurationGranularity durationGranularity = new DurationGranularity(finalBucketMillis, 0);

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
            SimpleTimeSeriesContainer.createFromInstance(
                buildDeltaSeries(simpleTimeSeriesContainer.computeSimple())
            )
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

      private SimpleTimeSeries buildDeltaSeries(SimpleTimeSeries simpleTimeSeries)
      {
        ImplyLongArrayList simpleTimeSeriesTimestamps = simpleTimeSeries.getTimestamps();
        ImplyDoubleArrayList simpleTimeSeriesDataPoints = simpleTimeSeries.getDataPoints();
        if (simpleTimeSeriesDataPoints.size() == 0) {
          return simpleTimeSeries;
        }
        ImplyLongArrayList deltaSeriesTimestamps = new ImplyLongArrayList();
        ImplyDoubleArrayList deltaSeriesDataPoints = new ImplyDoubleArrayList();

        long prevBucketStart = durationGranularity.bucketStart(simpleTimeSeriesTimestamps.getLong(0));
        double prevValue = simpleTimeSeriesDataPoints.getDouble(0);
        double runningSum = 0;
        boolean bucketInitialized = false;
        for (int i = 1; i < simpleTimeSeries.size(); i++) {
          double currValue = simpleTimeSeriesDataPoints.getDouble(i);
          if (currValue > prevValue) {
            long currTimestamp = simpleTimeSeriesTimestamps.getLong(i);
            long currBucketStart = durationGranularity.bucketStart(currTimestamp);
            runningSum += currValue - prevValue;
            if (currBucketStart > prevBucketStart) {
              tryAddToTimeseries(
                  deltaSeriesTimestamps,
                  deltaSeriesDataPoints,
                  simpleTimeSeriesTimestamps.getLong(i - 1),
                  prevBucketStart,
                  runningSum,
                  simpleTimeSeries
              );
              runningSum = 0;
              prevBucketStart = currBucketStart;
              bucketInitialized = false;
            } else {
              bucketInitialized = true;
            }
          }
          prevValue = currValue;
        }
        if (simpleTimeSeries.getEnd().getTimestamp() != -1 && simpleTimeSeries.getEnd().getData() > prevValue) {
          // if end exists and is a greater value than the last value in the timeseries, then add the delta
          runningSum += simpleTimeSeries.getEnd().getData() - prevValue;
          tryAddToTimeseries(
              deltaSeriesTimestamps,
              deltaSeriesDataPoints,
              simpleTimeSeriesTimestamps.getLong(simpleTimeSeriesTimestamps.size() - 1),
              prevBucketStart,
              runningSum,
              simpleTimeSeries
          );
        } else if (bucketInitialized) {
          // this means that the end is not a valid data point, so add to the series only if there's any delta to be collected
          tryAddToTimeseries(
              deltaSeriesTimestamps,
              deltaSeriesDataPoints,
              simpleTimeSeriesTimestamps.getLong(simpleTimeSeriesTimestamps.size() - 1),
              prevBucketStart,
              runningSum,
              simpleTimeSeries
          );
        }
        return new SimpleTimeSeries(
            deltaSeriesTimestamps,
            deltaSeriesDataPoints,
            simpleTimeSeries.getWindow(),
            null,
            null,
            simpleTimeSeries.getMaxEntries(),
            finalBucketMillis
        );
      }

      private void tryAddToTimeseries(
          ImplyLongArrayList timestamps,
          ImplyDoubleArrayList dataPoints,
          long currTimestamp,
          long currBucketStart,
          double dataPoint,
          SimpleTimeSeries inputSeries
      )
      {
        if (inputSeries.getWindow().contains(currBucketStart)) {
          timestamps.add(currBucketStart);
          dataPoints.add(dataPoint);
        } else {
          throw new IAE(
              "Found a delta recording (input timestamp : [%s], timestamp bucket : [%s]) outside the "
              + "window parameter of the resultant series. "
              + "Please align the bucketPeriod [%s] of delta timeseries and the window parameter [%s] of "
              + "input series such that the delta recordings are not outside the "
              + "input series' window period.",
              DateTimes.utc(currTimestamp),
              DateTimes.utc(currBucketStart),
              bucketPeriod,
              inputSeries.getWindow()
          );
        }
      }
    }
    return new DeltaTimeseriesExpr(arg);
  }

  @Override
  public String name()
  {
    return NAME;
  }
}
