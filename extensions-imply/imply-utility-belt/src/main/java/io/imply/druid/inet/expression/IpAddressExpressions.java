/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.expression;

import com.google.common.base.Preconditions;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.column.IpAddressBlob;
import io.imply.druid.inet.column.IpPrefixBlob;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.NamedFunction;
import org.apache.druid.segment.column.BaseTypeSignature;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IpAddressExpressions
{
  public static final ExpressionType IP_ADDRESS_TYPE = Preconditions.checkNotNull(
      ExpressionType.fromColumnType(IpAddressModule.ADDRESS_TYPE)
  );
  public static final ExpressionType IP_PREFIX_TYPE = Preconditions.checkNotNull(
      ExpressionType.fromColumnType(IpAddressModule.PREFIX_TYPE)
  );

  public static class AddressParseExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_parse";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class ParseExpr extends BaseParseExpr
      {
        public ParseExpr(Expr arg)
        {
          super(NAME, arg, IP_ADDRESS_TYPE);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return parse(bindings, true);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new ParseExpr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return IP_ADDRESS_TYPE;
        }
      }
      return new ParseExpr(args.get(0));
    }
  }

  public static class PrefixParseExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_prefix_parse";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class PrefixParseExpr extends BaseParseExpr
      {
        public PrefixParseExpr(Expr arg)
        {
          super(NAME, arg, IP_PREFIX_TYPE);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return parse(bindings, true);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new PrefixParseExpr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return IP_PREFIX_TYPE;
        }
      }
      return new PrefixParseExpr(args.get(0));
    }
  }

  public static class AddressTryParseExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_try_parse";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class TryParseExpr extends BaseParseExpr
      {
        public TryParseExpr(Expr arg)
        {
          super(NAME, arg, IP_ADDRESS_TYPE);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return parse(bindings, false);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new TryParseExpr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return IP_ADDRESS_TYPE;
        }
      }
      return new TryParseExpr(args.get(0));
    }
  }

  public static class PrefixTryParseExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_prefix_try_parse";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class PrefixTryParseExpr extends BaseParseExpr
      {
        public PrefixTryParseExpr(Expr arg)
        {
          super(NAME, arg, IP_PREFIX_TYPE);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return parse(bindings, false);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new PrefixTryParseExpr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return IP_PREFIX_TYPE;
        }
      }
      return new PrefixTryParseExpr(args.get(0));
    }
  }

  public static class StringifyExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_stringify";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class StringifyExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public StringifyExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          validateArgType(StringifyExprMacro.this, input, "address", IP_ADDRESS_TYPE, IP_PREFIX_TYPE);

          boolean compact = true;
          if (args.size() > 1) {
            compact = args.get(1).eval(bindings).asBoolean();
          }
          if (IP_ADDRESS_TYPE.equals(input.type())) {
            IpAddressBlob blob = (IpAddressBlob) input.value();
            if (blob == null) {
              return ExprEval.ofType(ExpressionType.STRING, null);
            }
            return ExprEval.ofType(ExpressionType.STRING, blob.stringify(compact, false));
          } else {
            IpPrefixBlob blob = (IpPrefixBlob) input.value();
            if (blob == null) {
              return ExprEval.ofType(ExpressionType.STRING, null);
            }
            return ExprEval.ofType(ExpressionType.STRING, blob.stringify(compact, false));
          }
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new StringifyExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING;
        }
      }

      return new StringifyExpr(args);
    }
  }

  public static class PrefixExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_prefix";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 2);

      if (args.get(1).isLiteral()) {
        int prefixLength = args.get(1).eval(InputBindings.nilBindings()).asInt();
        class PrefixExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
        {
          final int prefixLength;

          public PrefixExpr(List<Expr> args, int prefixLength)
          {
            super(NAME, args);
            this.prefixLength = prefixLength;
          }

          @Override
          public ExprEval eval(ObjectBinding bindings)
          {
            ExprEval input = args.get(0).eval(bindings);
            validateArgType(PrefixExprMacro.this, input, "address", IP_ADDRESS_TYPE);
            IpAddressBlob blob = (IpAddressBlob) input.value();
            if (blob == null) {
              return ExprEval.ofComplex(IP_PREFIX_TYPE, null);
            }
            return ExprEval.ofComplex(IP_PREFIX_TYPE, blob.toPrefix(prefixLength));
          }

          @Override
          public Expr visit(Shuttle shuttle)
          {
            List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
            return shuttle.visit(new PrefixExpr(newArgs, prefixLength));
          }

          @Nullable
          @Override
          public ExpressionType getOutputType(InputBindingInspector inspector)
          {
            return IP_PREFIX_TYPE;
          }
        }
        return new PrefixExpr(args, prefixLength);
      }

      class DynamicPrefixExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public DynamicPrefixExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          validateArgType(PrefixExprMacro.this, input, "address", IP_ADDRESS_TYPE);
          ExprEval prefixSize = args.get(1).eval(bindings);
          IpAddressBlob blob = (IpAddressBlob) input.value();
          if (blob == null) {
            return ExprEval.ofComplex(IP_PREFIX_TYPE, null);
          }
          int prefixLength = prefixSize.asInt();
          return ExprEval.ofComplex(IP_PREFIX_TYPE, blob.toPrefix(prefixLength));
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new DynamicPrefixExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return IP_PREFIX_TYPE;
        }
      }

      return new DynamicPrefixExpr(args);
    }
  }

  public static class HostExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_host";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class HostExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
      {
        public HostExpr(Expr arg)
        {
          super(NAME, arg);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = arg.eval(bindings);
          validateArgType(HostExprMacro.this, input, "prefix", IP_PREFIX_TYPE);
          IpPrefixBlob blob = (IpPrefixBlob) input.value();
          if (blob == null) {
            return ExprEval.ofComplex(IP_ADDRESS_TYPE, null);
          }
          return ExprEval.ofComplex(IP_ADDRESS_TYPE, blob.toHost());
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new HostExpr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return IP_ADDRESS_TYPE;
        }
      }
      return new HostExpr(args.get(0));
    }
  }

  public static class CompareExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_compare";

    /**
     * Comparator for {@link IpAddressBlob} used by this function. Cannot use {@link IpAddressBlob#compareTo} because
     * the builtin comparison is signed, not unsigned, and so would not sort IPs properly.
     */
    private static final Comparator<IpAddressBlob> COMPARATOR = (o1, o2) ->
        FrameReaderUtils.compareByteArraysUnsigned(
            o1.getBytes(),
            0,
            o1.getBytes().length,
            o2.getBytes(),
            0,
            o2.getBytes().length
        );

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(final List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 2);

      if (args.get(1).isLiteral()) {
        return new RhsLiteralCompareExpr(args);
      } else {
        return new DynamicCompareExpr(args);
      }
    }

    abstract class BaseCompareExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      public BaseCompareExpr(final List<Expr> args)
      {
        super(NAME, args);
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.LONG;
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }
    }

    /**
     * Expr when right-hand side is a literal.
     */
    class RhsLiteralCompareExpr extends BaseCompareExpr
    {
      private final Expr lhs;
      private final IpAddressBlob rhsAddress;

      public RhsLiteralCompareExpr(final List<Expr> args)
      {
        super(args);
        this.lhs = args.get(0);
        this.rhsAddress = literalAsBlob(args.get(1));
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        final ExprEval lhsEval = lhs.eval(bindings);
        validateArgType(CompareExprMacro.this, lhsEval, "lhs", IP_ADDRESS_TYPE);

        final IpAddressBlob lhsAddress = (IpAddressBlob) lhsEval.value();

        if (lhsAddress == null || rhsAddress == null) {
          return ExprEval.ofLong(null);
        }

        return ExprEval.ofLong(COMPARATOR.compare(lhsAddress, rhsAddress));
      }

      @Nullable
      private IpAddressBlob literalAsBlob(final Expr expr)
      {
        final Object val = expr.getLiteralValue();

        if (val == null) {
          return null;
        } else if (val instanceof String) {
          return IpAddressBlob.parse(val, true);
        } else if (val instanceof IpAddressBlob) {
          return (IpAddressBlob) val;
        } else {
          throw validationFailed(
              "requires type [%s] or [%s]",
              ExpressionType.STRING.asTypeString(),
              IP_ADDRESS_TYPE.asTypeString()
          );
        }
      }
    }

    /**
     * Expr when arguments are non-literal.
     */
    class DynamicCompareExpr extends BaseCompareExpr
    {
      public DynamicCompareExpr(final List<Expr> args)
      {
        super(args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        final ExprEval lhsEval = args.get(0).eval(bindings);
        validateArgType(CompareExprMacro.this, lhsEval, "lhs", IP_ADDRESS_TYPE);

        final ExprEval rhsEval = args.get(1).eval(bindings);
        validateArgType(CompareExprMacro.this, rhsEval, "rhs", IP_ADDRESS_TYPE);

        final IpAddressBlob lhsAddress = (IpAddressBlob) lhsEval.value();
        final IpAddressBlob rhsAddress = (IpAddressBlob) rhsEval.value();

        if (lhsAddress == null || rhsAddress == null) {
          return ExprEval.ofLong(null);
        }

        return ExprEval.ofLong(COMPARATOR.compare(lhsAddress, rhsAddress));
      }
    }
  }

  public static class MatchExprMacro extends BaseMatchExprMacro
  {
    public static final String NAME = "ip_match";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    Function<String, Boolean> getIpAddressMatchFunction(IpAddressBlob blob)
    {
      return blob::matches;
    }

    @Override
    Function<String, Boolean> getIpPrefixMatchFunction(IpPrefixBlob blob)
    {
      return blob::matches;
    }
  }

  public static class SearchExprMacro extends BaseMatchExprMacro
  {
    public static final String NAME = "ip_search";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    Function<String, Boolean> getIpAddressMatchFunction(IpAddressBlob blob)
    {
      return blob::searches;
    }

    @Override
    Function<String, Boolean> getIpPrefixMatchFunction(IpPrefixBlob blob)
    {
      return blob::searches;
    }
  }

  public static abstract class BaseMatchExprMacro implements ExprMacroTable.ExprMacro
  {
    abstract Function<String, Boolean> getIpAddressMatchFunction(IpAddressBlob blob);

    abstract Function<String, Boolean> getIpPrefixMatchFunction(IpPrefixBlob blob);

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 2);
      if (args.get(0).isLiteral() && args.get(1).isLiteral()) {
        throw new IAE(
            "Function[%s] must have exactly one of the two arguments be a Complex IP type: either a [%s] for the first argument, or a [%s] for the second argument.",
            name(),
            IP_ADDRESS_TYPE.asTypeString(),
            IP_PREFIX_TYPE.asTypeString()
        );
      }
      if (args.get(1).isLiteral()) {
        final ExprEval literalEval = args.get(1).eval(InputBindings.nilBindings());
        if (!literalEval.type().is(ExprType.STRING)) {
          throw new IAE(
              "Function[%s] second argument must be [%s] as input, got [%s]",
              name(),
              ExpressionType.STRING.asTypeString(),
              literalEval.type()
          );
        }
        final String literal = literalEval.asString();

        class MatchExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
        {
          public MatchExpr(List<Expr> args)
          {
            super(name(), args);
          }

          @Override
          public ExprEval eval(ObjectBinding bindings)
          {
            ExprEval input = args.get(0).eval(bindings);
            if (input.value() == null) {
              return ExprEval.ofLongBoolean(literal == null);
            } else if (literal == null) {
              return ExprEval.ofLongBoolean(input.value() == null);
            } else if (IP_ADDRESS_TYPE.equals(input.type())) {
              IpAddressBlob blob = (IpAddressBlob) input.value();
              if (blob == null) {
                return ExprEval.ofLongBoolean(false);
              }
              return ExprEval.ofLongBoolean(getIpAddressMatchFunction(blob).apply(literal));
            } else {
              throw new IAE(
                  "Function[%s] first argument is invalid type, got [%s] but expect [%s] since second argument is [%s]",
                  name,
                  input.type(),
                  IP_ADDRESS_TYPE.asTypeString(),
                  ExpressionType.STRING.asTypeString()
              );
            }
          }

          @Override
          public Expr visit(Shuttle shuttle)
          {
            List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
            return shuttle.visit(new MatchExpr(newArgs));
          }

          @Nullable
          @Override
          public ExpressionType getOutputType(InputBindingInspector inspector)
          {
            return ExpressionType.LONG;
          }
        }

        return new MatchExpr(args);
      }

      if (args.get(0).isLiteral()) {
        final ExprEval literalEval = args.get(0).eval(InputBindings.nilBindings());
        if (!literalEval.type().is(ExprType.STRING)) {
          throw new IAE(
              "Function[%s] first argument must be [%s] as input, got [%s]",
              name(),
              ExpressionType.STRING.asTypeString(),
              literalEval.type()
          );
        }
        final String literal = literalEval.asString();

        class MatchExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
        {
          public MatchExpr(List<Expr> args)
          {
            super(name(), args);
          }

          @Override
          public ExprEval eval(ObjectBinding bindings)
          {
            ExprEval input = args.get(1).eval(bindings);
            if (input.value() == null) {
              return ExprEval.ofLongBoolean(literal == null);
            } else if (literal == null) {
              return ExprEval.ofLongBoolean(input.value() == null);
            } else if (IP_PREFIX_TYPE.equals(input.type())) {
              IpPrefixBlob blob = (IpPrefixBlob) input.value();
              if (blob == null) {
                return ExprEval.ofLongBoolean(false);
              }
              return ExprEval.ofLongBoolean(getIpPrefixMatchFunction(blob).apply(literal));
            } else {
              throw new IAE(
                  "Function[%s] second argument is invalid type, got [%s] but expect [%s] since first argument is [%s]",
                  name,
                  input.type(),
                  IP_PREFIX_TYPE.asTypeString(),
                  ExpressionType.STRING.asTypeString()
              );
            }
          }

          @Override
          public Expr visit(Shuttle shuttle)
          {
            List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
            return shuttle.visit(new MatchExpr(newArgs));
          }

          @Nullable
          @Override
          public ExpressionType getOutputType(InputBindingInspector inspector)
          {
            return ExpressionType.LONG;
          }
        }

        return new MatchExpr(args);
      }

      class DynamicMatchExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public DynamicMatchExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          ExprEval matchesInput = args.get(1).eval(bindings);

          if ((IP_ADDRESS_TYPE.equals(input.type()) || input.value() == null) && (matchesInput.value() == null
                                                                                  || matchesInput.type()
                                                                                                 .is(ExprType.STRING))) {
            // The first argument is Ip Address Complex type (or null) and the second argument is String (or null)...
            IpAddressBlob blob = (IpAddressBlob) input.value();
            if (blob == null) {
              return ExprEval.ofLongBoolean(matchesInput.value() == null);
            }
            return ExprEval.ofLongBoolean(getIpAddressMatchFunction(blob).apply(matchesInput.asString()));
          } else if ((IP_PREFIX_TYPE.equals(matchesInput.type()) || matchesInput.value() == null) && (input.value()
                                                                                                      == null
                                                                                                      || input.type()
                                                                                                              .is(ExprType.STRING))) {
            // Or, the first argument is String (or null) and the second argument is Ip Prefix Complex type (or null)
            IpPrefixBlob blob = (IpPrefixBlob) matchesInput.value();
            if (blob == null) {
              return ExprEval.ofLongBoolean(input.value() == null);
            }
            return ExprEval.ofLongBoolean(getIpPrefixMatchFunction(blob).apply(input.asString()));
          } else {
            throw new IAE(
                "Function[%s] invalid arguments, got first argument [%s] and second argument [%s]. Must have exactly one of the two arguments be a Complex IP type: either a [%s] for the first argument, or a [%s] for the second argument.",
                name(),
                input.type(),
                matchesInput.type(),
                IP_ADDRESS_TYPE.asTypeString(),
                IP_PREFIX_TYPE.asTypeString()
            );
          }
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new DynamicMatchExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.LONG;
        }
      }

      return new DynamicMatchExpr(args);
    }
  }

  private abstract static class BaseParseExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
  {
    ExpressionType expressionType;

    public BaseParseExpr(String name, Expr arg, ExpressionType expressionType)
    {
      super(name, arg);
      this.expressionType = expressionType;
    }

    ExprEval parse(ObjectBinding bindings, boolean reportParseException)
    {
      ExprEval toParse = arg.eval(bindings);
      if (!toParse.type().is(ExprType.STRING)) {
        throw new IAE("Function[%s] must take a string as input, given [%s]", name, toParse.type().asTypeString());
      }
      if (expressionType.equals(IP_ADDRESS_TYPE)) {
        IpAddressBlob blob = IpAddressBlob.parse(toParse.asString(), reportParseException);
        return ExprEval.ofComplex(expressionType, blob);
      } else if (expressionType.equals(IP_PREFIX_TYPE)) {
        IpPrefixBlob blob = IpPrefixBlob.parse(toParse.asString(), reportParseException);
        return ExprEval.ofComplex(expressionType, blob);
      } else {
        throw new ISE("Invalid expressionType. Got expressionType=%s", expressionType.getClass().getName());
      }
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return expressionType;
    }
  }

  /**
   * Validate that an argument has an expected type (or one of a set of expected types).
   *
   * Could be moved to {@link NamedFunction}.
   *
   * @param namedFunction function being validated
   * @param eval          evaluated argument
   * @param argName       argument name (used for the error message)
   * @param types         expected types
   */
  private static void validateArgType(
      final NamedFunction namedFunction,
      final ExprEval<?> eval,
      final String argName,
      final ExpressionType... types
  )
  {
    if (eval.value() == null) {
      // Always allow null.
      return;
    }

    final ExpressionType evalType = eval.type();
    for (ExpressionType type : types) {
      if (type.equals(evalType)) {
        return;
      }
    }

    throw namedFunction.validationFailed(
        "argument[%s] must have type[%s]",
        argName,
        Arrays.stream(types).map(BaseTypeSignature::asTypeString).collect(Collectors.joining(" or "))
    );
  }
}
