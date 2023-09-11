/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.SimpleTimeSeriesUtils;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public abstract class ArithmeticTimeseriesExprMacro implements ExprMacroTable.ExprMacro
{
  private final Operator operator;
  private final String name;
  private static final ExpressionType OUTPUT_TYPE =
      Objects.requireNonNull(ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE), "type is null");

  public ArithmeticTimeseriesExprMacro(Operator operator)
  {
    this.operator = operator;
    this.name = StringUtils.format("%s_timeseries", operator.name().toLowerCase(Locale.ROOT));
  }

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 2, 3);
    boolean shouldNullPoison = false;
    if (args.size() == 3) {
      shouldNullPoison = Boolean.parseBoolean((String) TimeseriesExprUtil.expectLiteral(args.get(2), name(), 3));
    }

    boolean finalShouldNullPoison = shouldNullPoison;
    class ArithmeticOverTimeseriesExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      public ArithmeticOverTimeseriesExpr(List<Expr> args)
      {
        super(ArithmeticTimeseriesExprMacro.this.name(), args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object leftSeriesEvalValue = getArgs().get(0).eval(bindings).value();
        Object rightSeriesEvalValue = getArgs().get(1).eval(bindings).value();

        SimpleTimeSeriesContainer leftTimeSeriesContainer = (SimpleTimeSeriesContainer) leftSeriesEvalValue;
        SimpleTimeSeriesContainer rightTimeSeriesContainer = (SimpleTimeSeriesContainer) rightSeriesEvalValue;

        // check nulls and type validations
        if (leftTimeSeriesContainer == null || rightTimeSeriesContainer == null) {
          SimpleTimeSeriesContainer result = null;
          if (!finalShouldNullPoison) {
            result = operator.reduceNullSimpleTimeSeriesContainer(leftTimeSeriesContainer, rightTimeSeriesContainer);
          }
          return ExprEval.ofComplex(
              OUTPUT_TYPE,
              result == null ? SimpleTimeSeriesContainer.createFromInstance(null) : result
          );
        }

        if (!(leftSeriesEvalValue instanceof SimpleTimeSeriesContainer) ||
            !(rightSeriesEvalValue instanceof SimpleTimeSeriesContainer)
        ) {
          throw new IAE(
              "Function %s expects both arguments to be timeseries objects, but found objects of type [%s, %s]",
              leftSeriesEvalValue.getClass(),
              rightSeriesEvalValue.getClass()
          );
        }

        // check null timeseries containers
        if (leftTimeSeriesContainer.isNull() || rightTimeSeriesContainer.isNull()) {
          SimpleTimeSeriesContainer result = null;
          if (!finalShouldNullPoison) {
            result = operator.reduceNullSimpleTimeSeriesContainer(leftTimeSeriesContainer, rightTimeSeriesContainer);
          }
          return ExprEval.ofComplex(
              OUTPUT_TYPE,
              result == null ? SimpleTimeSeriesContainer.createFromInstance(null) : result
          );
        }
        SimpleTimeSeries leftTimeSeries = leftTimeSeriesContainer.computeSimple();
        SimpleTimeSeries rightTimeSeries = rightTimeSeriesContainer.computeSimple();

        // validate windows and timestamps
        SimpleTimeSeriesUtils.checkMatchingWindows(leftTimeSeries, rightTimeSeries, name());
        SimpleTimeSeriesUtils.checkMatchingSizeAndTimestamps(leftTimeSeries, rightTimeSeries, name());

        // reduce data points
        ImplyDoubleArrayList newDataPoints = new ImplyDoubleArrayList(
            operator.reduceDataPoints(
                leftTimeSeries.getDataPoints().getDoubleArray(),
                rightTimeSeries.getDataPoints().getDoubleArray(),
                leftTimeSeries.size()
            )
        );

        // copy timestamps
        long[] newTimestampsArr = new long[leftTimeSeries.size()];
        System.arraycopy(
            leftTimeSeries.getTimestamps().getLongArray(),
            0,
            newTimestampsArr,
            0,
            leftTimeSeries.size()
        );

        SimpleTimeSeries resultSeries = new SimpleTimeSeries(
            new ImplyLongArrayList(newTimestampsArr),
            newDataPoints,
            leftTimeSeries.getWindow(),
            mergeEdgePoints(leftTimeSeries.getStart(), rightTimeSeries.getStart(), true),
            mergeEdgePoints(leftTimeSeries.getEnd(), rightTimeSeries.getEnd(), false),
            Math.max(leftTimeSeries.getMaxEntries(), rightTimeSeries.getMaxEntries()),
            leftTimeSeries.getBucketMillis().equals(rightTimeSeries.getBucketMillis()) ?
              leftTimeSeries.getBucketMillis() : null
        );
        return ExprEval.ofComplex(
            OUTPUT_TYPE,
            SimpleTimeSeriesContainer.createFromInstance(resultSeries)
        );
      }

      private TimeSeries.EdgePoint mergeEdgePoints(
          TimeSeries.EdgePoint left,
          TimeSeries.EdgePoint right,
          boolean isStart
      )
      {
        if (left.getTimestamp() == -1) {
          return new TimeSeries.EdgePoint(right.getTimestamp(), right.getData());
        } else if (right.getTimestamp() == -1) {
          return new TimeSeries.EdgePoint(left.getTimestamp(), left.getData());
        } else {
          if (left.getTimestamp() == right.getTimestamp()) {
            return new TimeSeries.EdgePoint(
                left.getTimestamp(),
                operator.reduceDataPoints(new double[]{left.getData()}, new double[]{right.getData()}, 1)[0]
            );
          } else {
            boolean pickLeft = isStart ? left.getTimestamp() > right.getTimestamp() :
                               left.getTimestamp() < right.getTimestamp();
            return pickLeft ?
                   new TimeSeries.EdgePoint(left.getTimestamp(), left.getData()) :
                   new TimeSeries.EdgePoint(right.getTimestamp(), right.getData());
          }
        }
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
    return new ArithmeticOverTimeseriesExpr(args);
  }

  @Override
  public String name()
  {
    return name;
  }

  public enum Operator
  {
    ADD {
      @Override
      public double[] reduceDataPoints(double[] leftArr, double[] rightArr, int size)
      {
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
          result[i] = leftArr[i] + rightArr[i];
        }
        return result;
      }

      @Override
      public SimpleTimeSeriesContainer reduceNullSimpleTimeSeriesContainer(
          SimpleTimeSeriesContainer left,
          SimpleTimeSeriesContainer right
      )
      {
        // null + non-null = non-null. non-null + null = non-null
        if (left == null || left.isNull()) {
          return right;
        }
        if (right == null || right.isNull()) {
          return left;
        }
        throw ILLEGAL_ARGUMENT;
      }
    },
    SUBTRACT {
      @Override
      public double[] reduceDataPoints(double[] leftArr, double[] rightArr, int size)
      {
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
          result[i] = leftArr[i] - rightArr[i];
        }
        return result;
      }

      @Override
      public SimpleTimeSeriesContainer reduceNullSimpleTimeSeriesContainer(
          SimpleTimeSeriesContainer left,
          SimpleTimeSeriesContainer right
      )
      {
        // null - non-null = null. non-null - null = non-null
        if (left == null || left.isNull() || right == null || right.isNull()) {
          return left;
        }
        throw ILLEGAL_ARGUMENT;
      }
    },
    MULTIPLY {
      @Override
      public double[] reduceDataPoints(double[] leftArr, double[] rightArr, int size)
      {
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
          result[i] = leftArr[i] * rightArr[i];
        }
        return result;
      }

      @Override
      public SimpleTimeSeriesContainer reduceNullSimpleTimeSeriesContainer(
          SimpleTimeSeriesContainer left,
          SimpleTimeSeriesContainer right
      )
      {
        // null * non-null = null. non-null * null = null
        if (left == null || left.isNull() || right == null || right.isNull()) {
          return SimpleTimeSeriesContainer.createFromInstance(null);
        }
        throw ILLEGAL_ARGUMENT;
      }
    },
    DIVIDE {
      @Override
      public double[] reduceDataPoints(double[] leftArr, double[] rightArr, int size)
      {
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
          if (rightArr[i] == 0) {
            result[i] = Double.NaN; // converting divide by 0 to NaN to avoid blowing up everything
          }
          result[i] = leftArr[i] / rightArr[i];
        }
        return result;
      }

      @Override
      public SimpleTimeSeriesContainer reduceNullSimpleTimeSeriesContainer(
          SimpleTimeSeriesContainer left,
          SimpleTimeSeriesContainer right
      )
      {
        // null / non-null = null. non-null / null = non-null
        if (left == null || left.isNull() || right == null || right.isNull()) {
          return left;
        }
        throw ILLEGAL_ARGUMENT;
      }
    };

    private static final DruidException ILLEGAL_ARGUMENT =
        DruidException.forPersona(DruidException.Persona.DEVELOPER)
                      .ofCategory(DruidException.Category.DEFENSIVE)
                      .build("Method expects at least one argument to be null, but got 2 non-null arguments.  It is the caller's responsibility to check this.");

    public abstract double[] reduceDataPoints(double[] leftArr, double[] rightArr, int size);

    public abstract SimpleTimeSeriesContainer reduceNullSimpleTimeSeriesContainer(
        SimpleTimeSeriesContainer left,
        SimpleTimeSeriesContainer right
    );

    @Override
    @JsonValue
    public String toString()
    {
      return StringUtils.toUpperCase(name());
    }

    @Nullable
    @JsonCreator
    public static Operator fromString(String name)
    {
      return name == null ? null : valueOf(StringUtils.toUpperCase(name));
    }
  }

  public static class AddTimeseriesExprMacro extends ArithmeticTimeseriesExprMacro
  {
    public AddTimeseriesExprMacro()
    {
      super(Operator.ADD);
    }
  }

  public static class SubtractTimeseriesExprMacro extends ArithmeticTimeseriesExprMacro
  {
    public SubtractTimeseriesExprMacro()
    {
      super(Operator.SUBTRACT);
    }
  }

  public static class MultiplyTimeseriesExprMacro extends ArithmeticTimeseriesExprMacro
  {
    public MultiplyTimeseriesExprMacro()
    {
      super(Operator.MULTIPLY);
    }
  }

  public static class DivideTimeseriesExprMacro extends ArithmeticTimeseriesExprMacro
  {
    public DivideTimeseriesExprMacro()
    {
      super(Operator.DIVIDE);
    }
  }

  public static List<ArithmeticTimeseriesExprMacro> getMacros()
  {
    return ImmutableList.of(
        new AddTimeseriesExprMacro(),
        new SubtractTimeseriesExprMacro(),
        new MultiplyTimeseriesExprMacro(),
        new DivideTimeseriesExprMacro()
    );
  }
}
