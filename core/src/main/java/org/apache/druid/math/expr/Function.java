/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.math.expr;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.Expr.InputBindingInspector;
import org.apache.druid.math.expr.Expr.ObjectBinding;
import org.apache.druid.math.expr.vector.CastToTypeVectorProcessor;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorMathProcessors;
import org.apache.druid.math.expr.vector.VectorProcessors;
import org.apache.druid.math.expr.vector.VectorStringProcessors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base interface describing the mechanism used to evaluate a {@link FunctionExpr}. All {@link Function} implementations
 * are immutable.
 *
 * Do NOT remove "unused" members in this class. They are used by generated Antlr
 */
@SuppressWarnings("unused")
public interface Function
{
  /**
   * Name of the function.
   */
  String name();

  /**
   * Evaluate the function, given a list of arguments and a set of bindings to provide values for {@link IdentifierExpr}.
   */
  ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings);

  /**
   * Given a list of arguments to this {@link Function}, get the set of arguments that must evaluate to a scalar value
   */
  default Set<Expr> getScalarInputs(List<Expr> args)
  {
    return ImmutableSet.copyOf(args);
  }

  /**
   * Given a list of arguments to this {@link Function}, get the set of arguments that must evaluate to an array
   * value
   */
  default Set<Expr> getArrayInputs(List<Expr> args)
  {
    return Collections.emptySet();
  }

  /**
   * Returns true if a function expects any array arguments
   */
  default boolean hasArrayInputs()
  {
    return false;
  }

  /**
   * Returns true if function produces an array. All {@link Function} implementations are expected to
   * exclusively produce either scalar or array values.
   */
  default boolean hasArrayOutput()
  {
    return false;
  }

  /**
   * Validate function arguments
   */
  void validateArguments(List<Expr> args);

  /**
   * Compute the output type of this function for a given set of argument expression inputs.
   *
   * @see Expr#getOutputType
   */
  @Nullable
  ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args);

  /**
   * Check if a function can be 'vectorized', for a given set of {@link Expr} inputs. If this method returns true,
   * {@link #asVectorProcessor} is expected to produce a {@link ExprVectorProcessor} which can evaluate values in
   * batches to use with vectorized query engines.
   *
   * @see Expr#canVectorize(Expr.InputBindingInspector)
   * @see ApplyFunction#canVectorize(Expr.InputBindingInspector, Expr, List)
   */
  default boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
  {
    return false;
  }

  /**
   * Builds a 'vectorized' function expression processor, that can build vectorized processors for its input values
   * using {@link Expr#buildVectorized}, for use in vectorized query engines.
   *
   * @see Expr#buildVectorized(Expr.VectorInputBindingInspector)
   * @see ApplyFunction#asVectorProcessor(Expr.VectorInputBindingInspector, Expr, List)
   */
  default <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
  {
    throw new UOE("%s is not vectorized", name());
  }

  /**
   * Base class for a single variable input {@link Function} implementation
   */
  abstract class UnivariateFunction implements Function
  {
    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      Expr expr = args.get(0);
      return eval(expr.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval param);
  }

  /**
   * Base class for a 2 variable input {@link Function} implementation
   */
  abstract class BivariateFunction implements Function
  {
    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 arguments", name());
      }
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      Expr expr1 = args.get(0);
      Expr expr2 = args.get(1);
      return eval(expr1.eval(bindings), expr2.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval x, ExprEval y);
  }

  /**
   * Base class for a single variable input mathematical {@link Function}, with specialized 'eval' implementations that
   * that operate on primitive number types
   */
  abstract class UnivariateMathFunction extends UnivariateFunction
  {
    @Override
    protected final ExprEval eval(ExprEval param)
    {
      if (NullHandling.sqlCompatible() && param.isNumericNull()) {
        return ExprEval.of(null);
      }
      if (param.type() == ExprType.LONG) {
        return eval(param.asLong());
      } else if (param.type() == ExprType.DOUBLE) {
        return eval(param.asDouble());
      }
      return ExprEval.of(null);
    }

    protected ExprEval eval(long param)
    {
      return eval((double) param);
    }

    protected ExprEval eval(double param)
    {
      return eval((long) param);
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return args.get(0).getOutputType(inspector);
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return inspector.areNumeric(args) && inspector.canVectorize(args);
    }
  }

  /**
   * Many math functions always output a {@link Double} primitive, regardless of input type.
   */
  abstract class DoubleUnivariateMathFunction extends UnivariateMathFunction
  {
    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.DOUBLE;
    }
  }

  /**
   * Base class for a 2 variable input mathematical {@link Function}, with specialized 'eval' implementations that
   * operate on primitive number types
   */
  abstract class BivariateMathFunction extends BivariateFunction
  {
    @Override
    protected final ExprEval eval(ExprEval x, ExprEval y)
    {
      // match the logic of BinaryEvalOpExprBase.eval, except there is no string handling so both strings is also null
      if (NullHandling.sqlCompatible() && (x.value() == null || y.value() == null)) {
        return ExprEval.of(null);
      }

      ExprType type = ExprTypeConversion.autoDetect(x, y);
      switch (type) {
        case STRING:
          return ExprEval.of(null);
        case LONG:
          return eval(x.asLong(), y.asLong());
        case DOUBLE:
        default:
          return eval(x.asDouble(), y.asDouble());
      }
    }

    protected ExprEval eval(long x, long y)
    {
      return eval((double) x, (double) y);
    }

    protected ExprEval eval(double x, double y)
    {
      return eval((long) x, (long) y);
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprTypeConversion.function(
          args.get(0).getOutputType(inspector),
          args.get(1).getOutputType(inspector)
      );
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return inspector.areNumeric(args) && inspector.canVectorize(args);
    }
  }

  /**
   * Many math functions always output a {@link Double} primitive, regardless of input type.
   */
  abstract class DoubleBivariateMathFunction extends BivariateMathFunction
  {
    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.DOUBLE;
    }
  }

  abstract class BivariateBitwiseMathFunction extends BivariateFunction
  {
    @Override
    protected final ExprEval eval(ExprEval x, ExprEval y)
    {
      // this is a copy of the logic of BivariateMathFunction for string handling, which itself is a
      // remix of BinaryEvalOpExprBase.eval modified so that string inputs are always null outputs
      if (NullHandling.sqlCompatible() && (x.value() == null || y.value() == null)) {
        return ExprEval.of(null);
      }

      ExprType type = ExprTypeConversion.autoDetect(x, y);
      if (type == ExprType.STRING) {
        return ExprEval.of(null);
      }
      return eval(x.asLong(), y.asLong());
    }

    protected abstract ExprEval eval(long x, long y);

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return inspector.areNumeric(args) && inspector.canVectorize(args);
    }
  }

  /**
   * Base class for a 2 variable input {@link Function} whose first argument is a {@link ExprType#STRING} and second
   * argument is {@link ExprType#LONG}
   */
  abstract class StringLongFunction extends BivariateFunction
  {
    @Override
    protected final ExprEval eval(ExprEval x, ExprEval y)
    {
      if (x.type() != ExprType.STRING || y.type() != ExprType.LONG) {
        throw new IAE(
            "Function[%s] needs a string as first argument and an integer as second argument",
            name()
        );
      }
      return eval(x.asString(), y.asInt());
    }

    protected abstract ExprEval eval(@Nullable String x, int y);
  }

  /**
   * {@link Function} that takes 1 array operand and 1 scalar operand
   */
  abstract class ArrayScalarFunction implements Function
  {
    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 argument", name());
      }
    }

    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      return ImmutableSet.of(getScalarArgument(args));
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return ImmutableSet.of(getArrayArgument(args));
    }

    @Override
    public boolean hasArrayInputs()
    {
      return true;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval arrayExpr = getArrayArgument(args).eval(bindings);
      final ExprEval scalarExpr = getScalarArgument(args).eval(bindings);
      if (arrayExpr.asArray() == null) {
        return ExprEval.of(null);
      }
      return doApply(arrayExpr, scalarExpr);
    }

    Expr getScalarArgument(List<Expr> args)
    {
      return args.get(1);
    }

    Expr getArrayArgument(List<Expr> args)
    {
      return args.get(0);
    }

    abstract ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr);
  }

  /**
   * {@link Function} that takes 2 array operands
   */
  abstract class ArraysFunction implements Function
  {
    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 arguments", name());
      }
    }

    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      return Collections.emptySet();
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return ImmutableSet.copyOf(args);
    }

    @Override
    public boolean hasArrayInputs()
    {
      return true;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval arrayExpr1 = args.get(0).eval(bindings);
      final ExprEval arrayExpr2 = args.get(1).eval(bindings);

      if (arrayExpr1.asArray() == null) {
        return arrayExpr1;
      }
      if (arrayExpr2.asArray() == null) {
        return arrayExpr2;
      }

      return doApply(arrayExpr1, arrayExpr2);
    }

    abstract ExprEval doApply(ExprEval lhsExpr, ExprEval rhsExpr);
  }

  /**
   * Scaffolding for a 2 argument {@link Function} which accepts one array and one scalar input and adds the scalar
   * input to the array in some way.
   */
  abstract class ArrayAddElementFunction extends ArrayScalarFunction
  {
    @Override
    public boolean hasArrayOutput()
    {
      return true;
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      ExprType arrayType = getArrayArgument(args).getOutputType(inspector);
      return Optional.ofNullable(ExprType.asArrayType(arrayType)).orElse(arrayType);
    }

    @Override
    ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr)
    {
      switch (arrayExpr.type()) {
        case STRING:
        case STRING_ARRAY:
          return ExprEval.ofStringArray(add(arrayExpr.asStringArray(), scalarExpr.asString()).toArray(String[]::new));
        case LONG:
        case LONG_ARRAY:
          return ExprEval.ofLongArray(
              add(
                  arrayExpr.asLongArray(),
                  scalarExpr.isNumericNull() ? null : scalarExpr.asLong()
              ).toArray(Long[]::new)
          );
        case DOUBLE:
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(
              add(
                  arrayExpr.asDoubleArray(),
                  scalarExpr.isNumericNull() ? null : scalarExpr.asDouble()
              ).toArray(Double[]::new)
          );
      }

      throw new RE("Unable to add to unknown array type %s", arrayExpr.type());
    }

    abstract <T> Stream<T> add(T[] array, @Nullable T val);
  }

  /**
   * Base scaffolding for functions which accept 2 array arguments and combine them in some way
   */
  abstract class ArraysMergeFunction extends ArraysFunction
  {
    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return ImmutableSet.copyOf(args);
    }

    @Override
    public boolean hasArrayOutput()
    {
      return true;
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      ExprType arrayType = args.get(0).getOutputType(inspector);
      return Optional.ofNullable(ExprType.asArrayType(arrayType)).orElse(arrayType);
    }

    @Override
    ExprEval doApply(ExprEval lhsExpr, ExprEval rhsExpr)
    {
      final Object[] array1 = lhsExpr.asArray();
      final Object[] array2 = rhsExpr.asArray();

      if (array1 == null) {
        return ExprEval.of(null);
      }
      if (array2 == null) {
        return lhsExpr;
      }

      switch (lhsExpr.type()) {
        case STRING:
        case STRING_ARRAY:
          return ExprEval.ofStringArray(
              merge(lhsExpr.asStringArray(), rhsExpr.asStringArray()).toArray(String[]::new)
          );
        case LONG:
        case LONG_ARRAY:
          return ExprEval.ofLongArray(
              merge(lhsExpr.asLongArray(), rhsExpr.asLongArray()).toArray(Long[]::new)
          );
        case DOUBLE:
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(
              merge(lhsExpr.asDoubleArray(), rhsExpr.asDoubleArray()).toArray(Double[]::new)
          );
      }
      throw new RE("Unable to concatenate to unknown type %s", lhsExpr.type());
    }

    abstract <T> Stream<T> merge(T[] array1, T[] array2);
  }

  abstract class ReduceFunction implements Function
  {
    private final DoubleBinaryOperator doubleReducer;
    private final LongBinaryOperator longReducer;
    private final BinaryOperator<String> stringReducer;

    ReduceFunction(
        DoubleBinaryOperator doubleReducer,
        LongBinaryOperator longReducer,
        BinaryOperator<String> stringReducer
    )
    {
      this.doubleReducer = doubleReducer;
      this.longReducer = longReducer;
      this.stringReducer = stringReducer;
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      // anything goes
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      ExprType outputType = ExprType.LONG;
      for (Expr expr : args) {
        outputType = ExprTypeConversion.function(outputType, expr.getOutputType(inspector));
      }
      return outputType;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.isEmpty()) {
        return ExprEval.of(null);
      }

      // evaluate arguments and collect output type
      List<ExprEval<?>> evals = new ArrayList<>();
      ExprType outputType = ExprType.LONG;

      for (Expr expr : args) {
        ExprEval<?> exprEval = expr.eval(bindings);
        ExprType exprType = exprEval.type();

        if (isValidType(exprType)) {
          outputType = ExprTypeConversion.function(outputType, exprType);
        }

        if (exprEval.value() != null) {
          evals.add(exprEval);
        }
      }

      if (evals.isEmpty()) {
        // The GREATEST/LEAST functions are not in the SQL standard. Emulate the behavior of postgres (return null if
        // all expressions are null, otherwise skip null values) since it is used as a base for a wide number of
        // databases. This also matches the behavior the long/double greatest/least post aggregators. Some other
        // databases (e.g., MySQL) return null if any expression is null.
        // https://www.postgresql.org/docs/9.5/functions-conditional.html
        // https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_least
        return ExprEval.of(null);
      }

      switch (outputType) {
        case DOUBLE:
          //noinspection OptionalGetWithoutIsPresent (empty list handled earlier)
          return ExprEval.of(evals.stream().mapToDouble(ExprEval::asDouble).reduce(doubleReducer).getAsDouble());
        case LONG:
          //noinspection OptionalGetWithoutIsPresent (empty list handled earlier)
          return ExprEval.of(evals.stream().mapToLong(ExprEval::asLong).reduce(longReducer).getAsLong());
        default:
          //noinspection OptionalGetWithoutIsPresent (empty list handled earlier)
          return ExprEval.of(evals.stream().map(ExprEval::asString).reduce(stringReducer).get());
      }
    }

    private boolean isValidType(ExprType exprType)
    {
      switch (exprType) {
        case DOUBLE:
        case LONG:
        case STRING:
          return true;
        default:
          throw new IAE("Function[%s] does not accept %s types", name(), exprType);
      }
    }
  }

  // ------------------------------ implementations ------------------------------

  class ParseLong implements Function
  {
    @Override
    public String name()
    {
      return "parse_long";
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final int radix = args.size() == 1 ? 10 : args.get(1).eval(bindings).asInt();

      final String input = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
      if (input == null) {
        return ExprEval.ofLong(null);
      }

      final long retVal;
      try {
        if (radix == 16 && (input.startsWith("0x") || input.startsWith("0X"))) {
          // Strip leading 0x from hex strings.
          retVal = Long.parseLong(input.substring(2), radix);
        } else {
          retVal = Long.parseLong(input, radix);
        }
      }
      catch (NumberFormatException e) {
        return ExprEval.ofLong(null);
      }

      return ExprEval.of(retVal);
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return (args.size() == 1 || (args.get(1).isLiteral() && args.get(1).getLiteralValue() instanceof Number)) &&
             inspector.canVectorize(args);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      if (args.size() == 1 || args.get(1).isLiteral()) {
        final int radix = args.size() == 1 ? 10 : ((Number) args.get(1).getLiteralValue()).intValue();
        return VectorProcessors.parseLong(inspector, args.get(0), radix);
      }
      // only single argument and 2 argument where the radix is constant is currently implemented
      // the canVectorize check should prevent this from happening, but explode just in case
      throw Exprs.cannotVectorize(this);
    }
  }

  class Pi implements Function
  {
    private static final double PI = Math.PI;

    @Override
    public String name()
    {
      return "pi";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      return ExprEval.of(PI);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() > 0) {
        throw new IAE("Function[%s] needs 0 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.DOUBLE;
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return true;
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorProcessors.constantDouble(PI, inspector.getMaxVectorSize());
    }
  }

  class Abs extends UnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "abs";
    }

    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(Math.abs(param));
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.abs(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.abs(inspector, args.get(0));
    }
  }

  class Acos extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "acos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.acos(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.acos(inspector, args.get(0));
    }
  }

  class Asin extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "asin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.asin(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.asin(inspector, args.get(0));
    }
  }

  class Atan extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "atan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.atan(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.atan(inspector, args.get(0));
    }
  }

  class BitwiseComplement extends UnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseComplement";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(~param);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseComplement(inspector, args.get(0));
    }
  }

  class BitwiseConvertLongBitsToDouble extends UnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseConvertLongBitsToDouble";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      ExprType type = args.get(0).getOutputType(inspector);
      if (type == null) {
        return null;
      }
      return ExprType.DOUBLE;
    }

    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(Double.longBitsToDouble(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseConvertLongBitsToDouble(inspector, args.get(0));
    }
  }

  class BitwiseConvertDoubleToLongBits extends UnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseConvertDoubleToLongBits";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      ExprType type = args.get(0).getOutputType(inspector);
      if (type == null) {
        return null;
      }
      return ExprType.LONG;
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Double.doubleToLongBits(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseConvertDoubleToLongBits(inspector, args.get(0));
    }
  }

  class BitwiseAnd extends BivariateBitwiseMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseAnd";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(x & y);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseAnd(inspector, args.get(0), args.get(1));
    }
  }

  class BitwiseOr extends BivariateBitwiseMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseOr";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(x | y);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseOr(inspector, args.get(0), args.get(1));
    }
  }

  class BitwiseShiftLeft extends BivariateBitwiseMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseShiftLeft";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(x << y);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseShiftLeft(inspector, args.get(0), args.get(1));
    }
  }

  class BitwiseShiftRight extends BivariateBitwiseMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseShiftRight";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(x >> y);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseShiftRight(inspector, args.get(0), args.get(1));
    }
  }

  class BitwiseXor extends BivariateBitwiseMathFunction
  {
    @Override
    public String name()
    {
      return "bitwiseXor";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(x ^ y);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.bitwiseXor(inspector, args.get(0), args.get(1));
    }
  }

  class Cbrt extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "cbrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cbrt(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.cbrt(inspector, args.get(0));
    }
  }

  class Ceil extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "ceil";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ceil(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.ceil(inspector, args.get(0));
    }
  }

  class Cos extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "cos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.cos(inspector, args.get(0));
    }
  }

  class Cosh extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "cosh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cosh(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.cosh(inspector, args.get(0));
    }
  }

  class Cot extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "cot";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param) / Math.sin(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.cot(inspector, args.get(0));
    }
  }

  class Div extends BivariateMathFunction
  {
    @Override
    public String name()
    {
      return "div";
    }

    @Override
    protected ExprEval eval(final long x, final long y)
    {
      return ExprEval.of(x / y);
    }

    @Override
    protected ExprEval eval(final double x, final double y)
    {
      return ExprEval.of((long) (x / y));
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprTypeConversion.integerMathFunction(
          args.get(0).getOutputType(inspector),
          args.get(1).getOutputType(inspector)
      );
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.longDivide(inspector, args.get(0), args.get(1));
    }
  }

  class Exp extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "exp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.exp(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.exp(inspector, args.get(0));
    }
  }

  class Expm1 extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "expm1";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.expm1(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.expm1(inspector, args.get(0));
    }
  }

  class Floor extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "floor";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.floor(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.floor(inspector, args.get(0));
    }
  }

  class GetExponent extends UnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "getExponent";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.getExponent(param));
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.getExponent(inspector, args.get(0));
    }
  }

  class Log extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "log";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.log(inspector, args.get(0));
    }
  }

  class Log10 extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "log10";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log10(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.log10(inspector, args.get(0));
    }
  }

  class Log1p extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "log1p";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log1p(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.log1p(inspector, args.get(0));
    }
  }

  class NextUp extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "nextUp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.nextUp(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.nextUp(inspector, args.get(0));
    }
  }

  class Rint extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "rint";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.rint(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.rint(inspector, args.get(0));
    }
  }

  class Round implements Function
  {
    //CHECKSTYLE.OFF: Regexp
    private static final BigDecimal MAX_FINITE_VALUE = BigDecimal.valueOf(Double.MAX_VALUE);
    private static final BigDecimal MIN_FINITE_VALUE = BigDecimal.valueOf(-1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp

    @Override
    public String name()
    {
      return "round";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      ExprEval value1 = args.get(0).eval(bindings);

      if (NullHandling.sqlCompatible() && value1.isNumericNull()) {
        return ExprEval.of(null);
      }

      if (value1.type() != ExprType.LONG && value1.type() != ExprType.DOUBLE) {
        throw new IAE(
            "The first argument to the function[%s] should be integer or double type but got the type: %s",
            name(),
            value1.type()
        );
      }

      if (args.size() == 1) {
        return eval(value1);
      } else {
        ExprEval value2 = args.get(1).eval(bindings);
        if (value2.type() != ExprType.LONG) {
          throw new IAE(
              "The second argument to the function[%s] should be integer type but got the type: %s",
              name(),
              value2.type()
          );
        }
        return eval(value1, value2.asInt());
      }
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return args.get(0).getOutputType(inspector);
    }

    private ExprEval eval(ExprEval param)
    {
      return eval(param, 0);
    }

    private ExprEval eval(ExprEval param, int scale)
    {
      if (param.type() == ExprType.LONG) {
        return ExprEval.of(BigDecimal.valueOf(param.asLong()).setScale(scale, RoundingMode.HALF_UP).longValue());
      } else if (param.type() == ExprType.DOUBLE) {
        BigDecimal decimal = safeGetFromDouble(param.asDouble());
        return ExprEval.of(decimal.setScale(scale, RoundingMode.HALF_UP).doubleValue());
      } else {
        return ExprEval.of(null);
      }
    }

    /**
     * Converts non-finite doubles to BigDecimal values instead of throwing a NumberFormatException.
     */
    private static BigDecimal safeGetFromDouble(double val)
    {
      if (Double.isNaN(val)) {
        return BigDecimal.ZERO;
      } else if (val == Double.POSITIVE_INFINITY) {
        return MAX_FINITE_VALUE;
      } else if (val == Double.NEGATIVE_INFINITY) {
        return MIN_FINITE_VALUE;
      }
      return BigDecimal.valueOf(val);
    }
  }

  class Signum extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "signum";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.signum(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.signum(inspector, args.get(0));
    }
  }

  class Sin extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "sin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sin(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.sin(inspector, args.get(0));
    }
  }

  class Sinh extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "sinh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sinh(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.sinh(inspector, args.get(0));
    }
  }

  class Sqrt extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "sqrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sqrt(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.sqrt(inspector, args.get(0));
    }
  }

  class Tan extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "tan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tan(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.tan(inspector, args.get(0));
    }
  }

  class Tanh extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "tanh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tanh(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.tanh(inspector, args.get(0));
    }
  }

  class ToDegrees extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "toDegrees";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toDegrees(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.toDegrees(inspector, args.get(0));
    }
  }

  class ToRadians extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "toRadians";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toRadians(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.toRadians(inspector, args.get(0));
    }
  }

  class Ulp extends DoubleUnivariateMathFunction
  {
    @Override
    public String name()
    {
      return "ulp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ulp(param));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.ulp(inspector, args.get(0));
    }
  }

  class Atan2 extends DoubleBivariateMathFunction
  {
    @Override
    public String name()
    {
      return "atan2";
    }

    @Override
    protected ExprEval eval(double y, double x)
    {
      return ExprEval.of(Math.atan2(y, x));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.atan2(inspector, args.get(0), args.get(1));
    }
  }

  class CopySign extends DoubleBivariateMathFunction
  {
    @Override
    public String name()
    {
      return "copySign";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.copySign(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.copySign(inspector, args.get(0), args.get(1));
    }
  }

  class Hypot extends DoubleBivariateMathFunction
  {
    @Override
    public String name()
    {
      return "hypot";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.hypot(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.hypot(inspector, args.get(0), args.get(1));
    }
  }

  class Remainder extends DoubleBivariateMathFunction
  {
    @Override
    public String name()
    {
      return "remainder";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.IEEEremainder(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.remainder(inspector, args.get(0), args.get(1));
    }
  }

  class Max extends BivariateMathFunction
  {
    @Override
    public String name()
    {
      return "max";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.max(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.max(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.max(inspector, args.get(0), args.get(1));
    }
  }

  class Min extends BivariateMathFunction
  {
    @Override
    public String name()
    {
      return "min";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.min(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.min(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.min(inspector, args.get(0), args.get(1));
    }
  }

  class NextAfter extends DoubleBivariateMathFunction
  {
    @Override
    public String name()
    {
      return "nextAfter";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.nextAfter(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.nextAfter(inspector, args.get(0), args.get(1));
    }
  }

  class Pow extends DoubleBivariateMathFunction
  {
    @Override
    public String name()
    {
      return "pow";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.pow(x, y));
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.doublePower(inspector, args.get(0), args.get(1));
    }
  }

  class Scalb extends BivariateFunction
  {
    @Override
    public String name()
    {
      return "scalb";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.DOUBLE;
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      if (NullHandling.sqlCompatible() && (x.value() == null || y.value() == null)) {
        return ExprEval.of(null);
      }

      ExprType type = ExprTypeConversion.autoDetect(x, y);
      switch (type) {
        case STRING:
          return ExprEval.of(null);
        default:
          return ExprEval.of(Math.scalb(x.asDouble(), y.asInt()));
      }
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return inspector.areNumeric(args) && inspector.canVectorize(args);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return VectorMathProcessors.scalb(inspector, args.get(0), args.get(1));
    }
  }

  class CastFunc extends BivariateFunction
  {
    @Override
    public String name()
    {
      return "cast";
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      if (NullHandling.sqlCompatible() && x.value() == null) {
        return ExprEval.of(null);
      }
      ExprType castTo;
      try {
        castTo = ExprType.valueOf(StringUtils.toUpperCase(y.asString()));
      }
      catch (IllegalArgumentException e) {
        throw new IAE("invalid type '%s'", y.asString());
      }
      return x.castTo(castTo);
    }

    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      if (args.get(1).isLiteral()) {
        ExprType castTo = ExprType.valueOf(StringUtils.toUpperCase(args.get(1).getLiteralValue().toString()));
        switch (castTo) {
          case LONG_ARRAY:
          case DOUBLE_ARRAY:
          case STRING_ARRAY:
            return Collections.emptySet();
          default:
            return ImmutableSet.of(args.get(0));
        }
      }
      // unknown cast, can't safely assume either way
      return Collections.emptySet();
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      if (args.get(1).isLiteral()) {
        ExprType castTo = ExprType.valueOf(StringUtils.toUpperCase(args.get(1).getLiteralValue().toString()));
        switch (castTo) {
          case LONG:
          case DOUBLE:
          case STRING:
            return Collections.emptySet();
          default:
            return ImmutableSet.of(args.get(0));
        }
      }
      // unknown cast, can't safely assume either way
      return Collections.emptySet();
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      // can only know cast output type if cast to argument is constant
      if (args.get(1).isLiteral()) {
        return ExprType.valueOf(StringUtils.toUpperCase(args.get(1).getLiteralValue().toString()));
      }
      return null;
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return args.get(0).canVectorize(inspector) && args.get(1).isLiteral();
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(Expr.VectorInputBindingInspector inspector, List<Expr> args)
    {
      return CastToTypeVectorProcessor.cast(
          args.get(0).buildVectorized(inspector),
          ExprType.valueOf(StringUtils.toUpperCase(args.get(1).getLiteralValue().toString()))
      );
    }
  }

  class GreatestFunc extends ReduceFunction
  {
    public static final String NAME = "greatest";

    public GreatestFunc()
    {
      super(
          Math::max,
          Math::max,
          BinaryOperator.maxBy(Comparator.naturalOrder())
      );
    }

    @Override
    public String name()
    {
      return NAME;
    }
  }

  class LeastFunc extends ReduceFunction
  {
    public static final String NAME = "least";

    public LeastFunc()
    {
      super(
          Math::min,
          Math::min,
          BinaryOperator.minBy(Comparator.naturalOrder())
      );
    }

    @Override
    public String name()
    {
      return NAME;
    }
  }

  class ConditionFunc implements Function
  {
    @Override
    public String name()
    {
      return "if";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      ExprEval x = args.get(0).eval(bindings);
      return x.asBoolean() ? args.get(1).eval(bindings) : args.get(2).eval(bindings);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprTypeConversion.conditional(inspector, args.subList(1, 3));
    }
  }

  /**
   * "Searched CASE" function, similar to {@code CASE WHEN boolean_expr THEN result [ELSE else_result] END} in SQL.
   */
  class CaseSearchedFunc implements Function
  {
    @Override
    public String name()
    {
      return "case_searched";
    }

    @Override
    public ExprEval apply(final List<Expr> args, final Expr.ObjectBinding bindings)
    {
      for (int i = 0; i < args.size(); i += 2) {
        if (i == args.size() - 1) {
          // ELSE else_result.
          return args.get(i).eval(bindings);
        } else if (args.get(i).eval(bindings).asBoolean()) {
          // Matching WHEN boolean_expr THEN result
          return args.get(i + 1).eval(bindings);
        }
      }

      return ExprEval.of(null);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      List<Expr> results = new ArrayList<>();
      for (int i = 1; i < args.size(); i += 2) {
        results.add(args.get(i));
      }
      // add else
      results.add(args.get(args.size() - 1));
      return ExprTypeConversion.conditional(inspector, results);
    }
  }

  /**
   * "Simple CASE" function, similar to {@code CASE expr WHEN value THEN result [ELSE else_result] END} in SQL.
   */
  class CaseSimpleFunc implements Function
  {
    @Override
    public String name()
    {
      return "case_simple";
    }

    @Override
    public ExprEval apply(final List<Expr> args, final Expr.ObjectBinding bindings)
    {
      for (int i = 1; i < args.size(); i += 2) {
        if (i == args.size() - 1) {
          // ELSE else_result.
          return args.get(i).eval(bindings);
        } else if (new BinEqExpr("==", args.get(0), args.get(i)).eval(bindings).asBoolean()) {
          // Matching WHEN value THEN result
          return args.get(i + 1).eval(bindings);
        }
      }

      return ExprEval.of(null);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() < 3) {
        throw new IAE("Function[%s] must have at least 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      List<Expr> results = new ArrayList<>();
      for (int i = 2; i < args.size(); i += 2) {
        results.add(args.get(i));
      }
      // add else
      results.add(args.get(args.size() - 1));
      return ExprTypeConversion.conditional(inspector, results);
    }
  }

  class NvlFunc implements Function
  {
    @Override
    public String name()
    {
      return "nvl";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval eval = args.get(0).eval(bindings);
      return eval.value() == null ? args.get(1).eval(bindings) : eval;
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprTypeConversion.conditional(inspector, args);
    }
  }

  class IsNullFunc implements Function
  {
    @Override
    public String name()
    {
      return "isnull";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval expr = args.get(0).eval(bindings);
      return ExprEval.ofLongBoolean(expr.value() == null);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }
  }

  class IsNotNullFunc implements Function
  {
    @Override
    public String name()
    {
      return "notnull";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval expr = args.get(0).eval(bindings);
      return ExprEval.ofLongBoolean(expr.value() != null);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }
  }

  class ConcatFunc implements Function
  {
    @Override
    public String name()
    {
      return "concat";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() == 0) {
        return ExprEval.of(null);
      } else {
        // Pass first argument in to the constructor to provide StringBuilder a little extra sizing hint.
        String first = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
        if (first == null) {
          // Result of concatenation is null if any of the Values is null.
          // e.g. 'select CONCAT(null, "abc") as c;' will return null as per Standard SQL spec.
          return ExprEval.of(null);
        }
        final StringBuilder builder = new StringBuilder(first);
        for (int i = 1; i < args.size(); i++) {
          final String s = NullHandling.nullToEmptyIfNeeded(args.get(i).eval(bindings).asString());
          if (s == null) {
            // Result of concatenation is null if any of the Values is null.
            // e.g. 'select CONCAT(null, "abc") as c;' will return null as per Standard SQL spec.
            return ExprEval.of(null);
          } else {
            builder.append(s);
          }
        }
        return ExprEval.of(builder.toString());
      }
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      // anything goes
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }

    @Override
    public boolean canVectorize(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return inspector.areScalar(args) && inspector.canVectorize(args);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(
        Expr.VectorInputBindingInspector inspector,
        List<Expr> args
    )
    {
      return VectorStringProcessors.concat(inspector, args);
    }
  }

  class StrlenFunc implements Function
  {
    @Override
    public String name()
    {
      return "strlen";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String arg = args.get(0).eval(bindings).asString();
      return arg == null ? ExprEval.ofLong(NullHandling.defaultLongValue()) : ExprEval.of(arg.length());
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }
  }

  class StringFormatFunc implements Function
  {
    @Override
    public String name()
    {
      return "format";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String formatString = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());

      if (formatString == null) {
        return ExprEval.of(null);
      }

      final Object[] formatArgs = new Object[args.size() - 1];
      for (int i = 1; i < args.size(); i++) {
        formatArgs[i - 1] = args.get(i).eval(bindings).value();
      }

      return ExprEval.of(StringUtils.nonStrictFormat(formatString, formatArgs));
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() < 1) {
        throw new IAE("Function[%s] needs 1 or more arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class StrposFunc implements Function
  {
    @Override
    public String name()
    {
      return "strpos";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String haystack = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
      final String needle = NullHandling.nullToEmptyIfNeeded(args.get(1).eval(bindings).asString());

      if (haystack == null || needle == null) {
        return ExprEval.of(null);
      }

      final int fromIndex;

      if (args.size() >= 3) {
        fromIndex = args.get(2).eval(bindings).asInt();
      } else {
        fromIndex = 0;
      }

      return ExprEval.of(haystack.indexOf(needle, fromIndex));
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() < 2 || args.size() > 3) {
        throw new IAE("Function[%s] needs 2 or 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }
  }

  class SubstringFunc implements Function
  {
    @Override
    public String name()
    {
      return "substring";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String arg = args.get(0).eval(bindings).asString();

      if (arg == null) {
        return ExprEval.of(null);
      }

      // Behaves like SubstringDimExtractionFn, not SQL SUBSTRING
      final int index = args.get(1).eval(bindings).asInt();
      final int length = args.get(2).eval(bindings).asInt();

      if (index < arg.length()) {
        if (length >= 0) {
          return ExprEval.of(arg.substring(index, Math.min(index + length, arg.length())));
        } else {
          return ExprEval.of(arg.substring(index));
        }
      } else {
        // If starting index of substring is greater then the length of string, the result will be a zero length string.
        // e.g. 'select substring("abc", 4,5) as c;' will return an empty string
        return ExprEval.of(NullHandling.defaultStringValue());
      }
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class RightFunc extends StringLongFunction
  {
    @Override
    public String name()
    {
      return "right";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }

    @Override
    protected ExprEval eval(@Nullable String x, int y)
    {
      if (y < 0) {
        throw new IAE(
            "Function[%s] needs a postive integer as second argument",
            name()
        );
      }
      if (x == null) {
        return ExprEval.of(null);
      }
      int len = x.length();
      return ExprEval.of(y < len ? x.substring(len - y) : x);
    }
  }

  class LeftFunc extends StringLongFunction
  {
    @Override
    public String name()
    {
      return "left";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }

    @Override
    protected ExprEval eval(@Nullable String x, int y)
    {
      if (y < 0) {
        throw new IAE(
            "Function[%s] needs a postive integer as second argument",
            name()
        );
      }
      if (x == null) {
        return ExprEval.of(null);
      }
      return ExprEval.of(y < x.length() ? x.substring(0, y) : x);
    }
  }

  class ReplaceFunc implements Function
  {
    @Override
    public String name()
    {
      return "replace";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String arg = args.get(0).eval(bindings).asString();
      final String pattern = NullHandling.nullToEmptyIfNeeded(args.get(1).eval(bindings).asString());
      final String replacement = NullHandling.nullToEmptyIfNeeded(args.get(2).eval(bindings).asString());
      if (arg == null) {
        return ExprEval.of(NullHandling.defaultStringValue());
      }
      return ExprEval.of(StringUtils.replace(arg, pattern, replacement));
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class LowerFunc implements Function
  {
    @Override
    public String name()
    {
      return "lower";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String arg = args.get(0).eval(bindings).asString();
      if (arg == null) {
        return ExprEval.of(NullHandling.defaultStringValue());
      }
      return ExprEval.of(StringUtils.toLowerCase(arg));
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class UpperFunc implements Function
  {
    @Override
    public String name()
    {
      return "upper";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final String arg = args.get(0).eval(bindings).asString();
      if (arg == null) {
        return ExprEval.of(NullHandling.defaultStringValue());
      }
      return ExprEval.of(StringUtils.toUpperCase(arg));
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class ReverseFunc extends UnivariateFunction
  {
    @Override
    public String name()
    {
      return "reverse";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }

    @Override
    protected ExprEval eval(ExprEval param)
    {
      if (param.type() != ExprType.STRING) {
        throw new IAE(
            "Function[%s] needs a string argument",
            name()
        );
      }
      final String arg = param.asString();
      return ExprEval.of(arg == null ? NullHandling.defaultStringValue() : new StringBuilder(arg).reverse().toString());
    }
  }

  class RepeatFunc extends StringLongFunction
  {
    @Override
    public String name()
    {
      return "repeat";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }

    @Override
    protected ExprEval eval(String x, int y)
    {
      if (x == null) {
        return ExprEval.of(null);
      }
      return ExprEval.of(y < 1 ? NullHandling.defaultStringValue() : StringUtils.repeat(x, y));
    }
  }

  class LpadFunc implements Function
  {
    @Override
    public String name()
    {
      return "lpad";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      String base = args.get(0).eval(bindings).asString();
      int len = args.get(1).eval(bindings).asInt();
      String pad = args.get(2).eval(bindings).asString();

      if (base == null || pad == null) {
        return ExprEval.of(null);
      } else {
        return ExprEval.of(len == 0 ? NullHandling.defaultStringValue() : StringUtils.lpad(base, len, pad));
      }

    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class RpadFunc implements Function
  {
    @Override
    public String name()
    {
      return "rpad";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      String base = args.get(0).eval(bindings).asString();
      int len = args.get(1).eval(bindings).asInt();
      String pad = args.get(2).eval(bindings).asString();

      if (base == null || pad == null) {
        return ExprEval.of(null);
      } else {
        return ExprEval.of(len == 0 ? NullHandling.defaultStringValue() : StringUtils.rpad(base, len, pad));
      }

    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class TimestampFromEpochFunc implements Function
  {
    @Override
    public String name()
    {
      return "timestamp";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      ExprEval value = args.get(0).eval(bindings);
      if (value.type() != ExprType.STRING) {
        throw new IAE("first argument should be string type but got %s type", value.type());
      }

      DateTimes.UtcFormatter formatter = DateTimes.ISO_DATE_OPTIONAL_TIME;
      if (args.size() > 1) {
        ExprEval format = args.get(1).eval(bindings);
        if (format.type() != ExprType.STRING) {
          throw new IAE("second argument should be string type but got %s type", format.type());
        }
        formatter = DateTimes.wrapFormatter(DateTimeFormat.forPattern(format.asString()));
      }
      DateTime date;
      try {
        date = formatter.parse(value.asString());
      }
      catch (IllegalArgumentException e) {
        throw new IAE(e, "invalid value %s", value.asString());
      }
      return toValue(date);
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    protected ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis());
    }
  }

  class UnixTimestampFunc extends TimestampFromEpochFunc
  {
    @Override
    public String name()
    {
      return "unix_timestamp";
    }

    @Override
    protected final ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis() / 1000);
    }
  }

  class SubMonthFunc implements Function
  {
    @Override
    public String name()
    {
      return "subtract_months";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      Long left = args.get(0).eval(bindings).asLong();
      Long right = args.get(1).eval(bindings).asLong();
      DateTimeZone timeZone = DateTimes.inferTzFromString(args.get(2).eval(bindings).asString());

      if (left == null || right == null) {
        return ExprEval.of(null);
      } else {
        return ExprEval.of(DateTimes.subMonths(right, left, timeZone));
      }

    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }
  }

  class ArrayConstructorFunction implements Function
  {
    @Override
    public String name()
    {
      return "array";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      // this is copied from 'BaseMapFunction.applyMap', need to find a better way to consolidate, or construct arrays,
      // or.. something...
      final int length = args.size();
      String[] stringsOut = null;
      Long[] longsOut = null;
      Double[] doublesOut = null;

      ExprType elementType = null;
      for (int i = 0; i < length; i++) {
        ExprEval<?> evaluated = args.get(i).eval(bindings);
        if (elementType == null) {
          elementType = evaluated.type();
          switch (elementType) {
            case STRING:
              stringsOut = new String[length];
              break;
            case LONG:
              longsOut = new Long[length];
              break;
            case DOUBLE:
              doublesOut = new Double[length];
              break;
            default:
              throw new RE("Unhandled array constructor element type [%s]", elementType);
          }
        }

        setArrayOutputElement(stringsOut, longsOut, doublesOut, elementType, i, evaluated);
      }

      // There should be always at least one argument and thus elementType is never null.
      // See validateArguments().
      //noinspection ConstantConditions
      switch (elementType) {
        case STRING:
          return ExprEval.ofStringArray(stringsOut);
        case LONG:
          return ExprEval.ofLongArray(longsOut);
        case DOUBLE:
          return ExprEval.ofDoubleArray(doublesOut);
        default:
          throw new RE("Unhandled array constructor element type [%s]", elementType);
      }
    }

    static void setArrayOutputElement(
        String[] stringsOut,
        Long[] longsOut,
        Double[] doublesOut,
        ExprType elementType,
        int i,
        ExprEval evaluated
    )
    {
      switch (elementType) {
        case STRING:
          stringsOut[i] = evaluated.asString();
          break;
        case LONG:
          longsOut[i] = evaluated.isNumericNull() ? null : evaluated.asLong();
          break;
        case DOUBLE:
          doublesOut[i] = evaluated.isNumericNull() ? null : evaluated.asDouble();
          break;
      }
    }


    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      return ImmutableSet.copyOf(args);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return Collections.emptySet();
    }

    @Override
    public boolean hasArrayOutput()
    {
      return true;
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.isEmpty()) {
        throw new IAE("Function[%s] needs at least 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      ExprType type = ExprType.LONG;
      for (Expr arg : args) {
        type = ExprTypeConversion.function(type, arg.getOutputType(inspector));
      }
      return ExprType.asArrayType(type);
    }
  }

  class ArrayLengthFunction implements Function
  {
    @Override
    public String name()
    {
      return "array_length";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval expr = args.get(0).eval(bindings);
      final Object[] array = expr.asArray();
      if (array == null) {
        return ExprEval.of(null);
      }

      return ExprEval.ofLong(array.length);
    }


    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
      return ImmutableSet.of(args.get(0));
    }

    @Override
    public boolean hasArrayInputs()
    {
      return true;
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      return Collections.emptySet();
    }
  }

  class StringToArrayFunction implements Function
  {
    @Override
    public String name()
    {
      return "string_to_array";
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING_ARRAY;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval expr = args.get(0).eval(bindings);
      final String arrayString = expr.asString();
      if (arrayString == null) {
        return ExprEval.of(null);
      }

      final String split = args.get(1).eval(bindings).asString();
      return ExprEval.ofStringArray(arrayString.split(split != null ? split : ""));
    }

    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      return ImmutableSet.copyOf(args);
    }

    @Override
    public boolean hasArrayOutput()
    {
      return true;
    }
  }

  class ArrayToStringFunction extends ArrayScalarFunction
  {
    @Override
    public String name()
    {
      return "array_to_string";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }

    @Override
    ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr)
    {
      final String join = scalarExpr.asString();
      final Object[] raw = arrayExpr.asArray();
      if (raw == null || raw.length == 1 && raw[0] == null) {
        return ExprEval.of(null);
      }
      return ExprEval.of(
          Arrays.stream(raw).map(String::valueOf).collect(Collectors.joining(join != null ? join : ""))
      );
    }
  }

  class ArrayOffsetFunction extends ArrayScalarFunction
  {
    @Override
    public String name()
    {
      return "array_offset";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.elementType(args.get(0).getOutputType(inspector));
    }

    @Override
    ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr)
    {
      final Object[] array = arrayExpr.asArray();
      final int position = scalarExpr.asInt();

      if (array.length > position) {
        return ExprEval.bestEffortOf(array[position]);
      }
      return ExprEval.of(null);
    }
  }

  class ArrayOrdinalFunction extends ArrayScalarFunction
  {
    @Override
    public String name()
    {
      return "array_ordinal";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.elementType(args.get(0).getOutputType(inspector));
    }

    @Override
    ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr)
    {
      final Object[] array = arrayExpr.asArray();
      final int position = scalarExpr.asInt() - 1;

      if (array.length > position) {
        return ExprEval.bestEffortOf(array[position]);
      }
      return ExprEval.of(null);
    }
  }

  class ArrayOffsetOfFunction extends ArrayScalarFunction
  {
    @Override
    public String name()
    {
      return "array_offset_of";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr)
    {
      final Object[] array = arrayExpr.asArray();

      switch (scalarExpr.type()) {
        case STRING:
        case LONG:
        case DOUBLE:
          int index = -1;
          for (int i = 0; i < array.length; i++) {
            if (Objects.equals(array[i], scalarExpr.value())) {
              index = i;
              break;
            }
          }
          return index < 0 ? ExprEval.ofLong(NullHandling.replaceWithDefault() ? -1 : null) : ExprEval.ofLong(index);
        default:
          throw new IAE("Function[%s] 2nd argument must be a a scalar type", name());
      }
    }
  }

  class ArrayOrdinalOfFunction extends ArrayScalarFunction
  {
    @Override
    public String name()
    {
      return "array_ordinal_of";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    ExprEval doApply(ExprEval arrayExpr, ExprEval scalarExpr)
    {
      final Object[] array = arrayExpr.asArray();
      switch (scalarExpr.type()) {
        case STRING:
        case LONG:
        case DOUBLE:
          int index = -1;
          for (int i = 0; i < array.length; i++) {
            if (Objects.equals(array[i], scalarExpr.value())) {
              index = i;
              break;
            }
          }
          return index < 0 ? ExprEval.ofLong(NullHandling.replaceWithDefault() ? -1 : null) : ExprEval.ofLong(index + 1);
        default:
          throw new IAE("Function[%s] 2nd argument must be a a scalar type", name());
      }
    }
  }

  class ArrayAppendFunction extends ArrayAddElementFunction
  {
    @Override
    public String name()
    {
      return "array_append";
    }

    @Override
    <T> Stream<T> add(T[] array, @Nullable T val)
    {
      List<T> l = new ArrayList<>(Arrays.asList(array));
      l.add(val);
      return l.stream();
    }
  }

  class ArrayPrependFunction extends ArrayAddElementFunction
  {
    @Override
    public String name()
    {
      return "array_prepend";
    }

    @Override
    Expr getScalarArgument(List<Expr> args)
    {
      return args.get(0);
    }

    @Override
    Expr getArrayArgument(List<Expr> args)
    {
      return args.get(1);
    }

    @Override
    <T> Stream<T> add(T[] array, @Nullable T val)
    {
      List<T> l = new ArrayList<>(Arrays.asList(array));
      l.add(0, val);
      return l.stream();
    }
  }

  class ArrayConcatFunction extends ArraysMergeFunction
  {
    @Override
    public String name()
    {
      return "array_concat";
    }

    @Override
    <T> Stream<T> merge(T[] array1, T[] array2)
    {
      List<T> l = new ArrayList<>(Arrays.asList(array1));
      l.addAll(Arrays.asList(array2));
      return l.stream();
    }
  }

  class ArraySetAddFunction extends ArrayAddElementFunction
  {
    @Override
    public String name()
    {
      return "array_set_add";
    }

    @Override
    <T> Stream<T> add(T[] array, @Nullable T val)
    {
      Set<T> l = new HashSet<>(Arrays.asList(array));
      l.add(val);
      return l.stream();
    }
  }

  class ArraySetAddAllFunction extends ArraysMergeFunction
  {
    @Override
    public String name()
    {
      return "array_set_add_all";
    }

    @Override
    <T> Stream<T> merge(T[] array1, T[] array2)
    {
      Set<T> l = new HashSet<>(Arrays.asList(array1));
      l.addAll(Arrays.asList(array2));
      return l.stream();
    }
  }

  class ArrayContainsFunction extends ArraysFunction
  {
    @Override
    public String name()
    {
      return "array_contains";
    }

    @Override
    public boolean hasArrayOutput()
    {
      return true;
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    ExprEval doApply(ExprEval lhsExpr, ExprEval rhsExpr)
    {
      final Object[] array1 = lhsExpr.asArray();
      final Object[] array2 = rhsExpr.asArray();
      return ExprEval.ofLongBoolean(Arrays.asList(array1).containsAll(Arrays.asList(array2)));
    }
  }

  class ArrayOverlapFunction extends ArraysFunction
  {
    @Override
    public String name()
    {
      return "array_overlap";
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.LONG;
    }

    @Override
    ExprEval doApply(ExprEval lhsExpr, ExprEval rhsExpr)
    {
      final Object[] array1 = lhsExpr.asArray();
      final List<Object> array2 = Arrays.asList(rhsExpr.asArray());
      boolean any = false;
      for (Object check : array1) {
        any |= array2.contains(check);
      }
      return ExprEval.ofLongBoolean(any);
    }
  }

  class ArraySliceFunction implements Function
  {
    @Override
    public String name()
    {
      return "array_slice";
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IAE("Function[%s] needs 2 or 3 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inspector, List<Expr> args)
    {
      return args.get(0).getOutputType(inspector);
    }

    @Override
    public Set<Expr> getScalarInputs(List<Expr> args)
    {
      if (args.size() == 3) {
        return ImmutableSet.of(args.get(1), args.get(2));
      } else {
        return ImmutableSet.of(args.get(1));
      }
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return ImmutableSet.of(args.get(0));
    }

    @Override
    public boolean hasArrayInputs()
    {
      return true;
    }

    @Override
    public boolean hasArrayOutput()
    {
      return true;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval expr = args.get(0).eval(bindings);
      final Object[] array = expr.asArray();
      if (array == null) {
        return ExprEval.of(null);
      }

      final int start = args.get(1).eval(bindings).asInt();
      int end = array.length;
      if (args.size() == 3) {
        end = args.get(2).eval(bindings).asInt();
      }

      if (start < 0 || start > array.length || start > end) {
        // Arrays.copyOfRange will throw exception in these cases
        return ExprEval.of(null);
      }

      switch (expr.type()) {
        case STRING:
        case STRING_ARRAY:
          return ExprEval.ofStringArray(Arrays.copyOfRange(expr.asStringArray(), start, end));
        case LONG:
        case LONG_ARRAY:
          return ExprEval.ofLongArray(Arrays.copyOfRange(expr.asLongArray(), start, end));
        case DOUBLE:
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(Arrays.copyOfRange(expr.asDoubleArray(), start, end));
      }
      throw new RE("Unable to slice to unknown type %s", expr.type());
    }
  }

  abstract class SizeFormatFunc implements Function
  {
    protected abstract HumanReadableBytes.UnitSystem getUnitSystem();

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final ExprEval valueParam = args.get(0).eval(bindings);
      if (NullHandling.sqlCompatible() && valueParam.isNumericNull()) {
        return ExprEval.of(null);
      }

      /**
       * only LONG and DOUBLE are allowed
       * For a DOUBLE, it will be cast to LONG before format
       */
      if (valueParam.value() != null && valueParam.type() != ExprType.LONG && valueParam.type() != ExprType.DOUBLE) {
        throw new IAE("Function[%s] needs a number as its first argument", name());
      }

      /**
       * By default, precision is 2
       */
      long precision = 2;
      if (args.size() > 1) {
        ExprEval precisionParam = args.get(1).eval(bindings);
        if (precisionParam.type() != ExprType.LONG) {
          throw new IAE("Function[%s] needs an integer as its second argument", name());
        }
        precision = precisionParam.asLong();
        if (precision < 0 || precision > 3) {
          throw new IAE("Given precision[%d] of Function[%s] must be in the range of [0,3]", precision, name());
        }
      }

      return ExprEval.of(HumanReadableBytes.format(valueParam.asLong(), precision, this.getUnitSystem()));
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() < 1 || args.size() > 2) {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(Expr.InputBindingInspector inputTypes, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }

  class HumanReadableDecimalByteFormatFunc extends SizeFormatFunc
  {
    @Override
    public String name()
    {
      return "human_readable_decimal_byte_format";
    }

    @Override
    protected HumanReadableBytes.UnitSystem getUnitSystem()
    {
      return HumanReadableBytes.UnitSystem.DECIMAL_BYTE;
    }
  }

  class HumanReadableBinaryByteFormatFunc extends SizeFormatFunc
  {
    @Override
    public String name()
    {
      return "human_readable_binary_byte_format";
    }

    @Override
    protected HumanReadableBytes.UnitSystem getUnitSystem()
    {
      return HumanReadableBytes.UnitSystem.BINARY_BYTE;
    }
  }

  class HumanReadableDecimalFormatFunc extends SizeFormatFunc
  {
    @Override
    public String name()
    {
      return "human_readable_decimal_format";
    }

    @Override
    protected HumanReadableBytes.UnitSystem getUnitSystem()
    {
      return HumanReadableBytes.UnitSystem.DECIMAL;
    }
  }

  /**
   * This function makes the current thread sleep for the given amount of seconds.
   * Fractional-second delays can be specified.
   *
   * This function is applied per row. The actual query time can vary depending on how much parallelism is used
   * for the query. As it does not provide consistent sleep time, this function should be used only for testing
   * when you want to keep a certain query running during the test.
   */
  @VisibleForTesting
  class Sleep implements Function
  {
    @Override
    public String name()
    {
      return "sleep";
    }

    @Override
    public ExprEval apply(List<Expr> args, ObjectBinding bindings)
    {
      ExprEval eval = args.get(0).eval(bindings);
      try {
        if (!eval.isNumericNull()) {
          double seconds = eval.asDouble();
          if (seconds > 0) {
            Thread.sleep((long) (seconds * 1000));
          }
        }
        return ExprEval.of(null);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void validateArguments(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
    }

    @Nullable
    @Override
    public ExprType getOutputType(InputBindingInspector inspector, List<Expr> args)
    {
      return ExprType.STRING;
    }
  }
}
