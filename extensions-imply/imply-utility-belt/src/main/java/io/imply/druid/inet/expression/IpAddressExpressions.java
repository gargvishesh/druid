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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class IpAddressExpressions
{
  public static final ExpressionType TYPE = Preconditions.checkNotNull(
      ExpressionType.fromColumnType(IpAddressModule.TYPE)
  );

  public static class ParseExprMacro implements ExprMacroTable.ExprMacro
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
          super(NAME, arg);
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
      }
      return new ParseExpr(args.get(0));
    }
  }

  public static class TryParseExprMacro implements ExprMacroTable.ExprMacro
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
          super(NAME, arg);
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
      }
      return new TryParseExpr(args.get(0));
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
          if (!TYPE.equals(input.type()) && input.value() != null) {
            throw new IAE("Function[%s] must take [%s] as input", name, TYPE.asTypeString());
          }
          boolean compact = true;
          if (args.size() > 1) {
            compact = args.get(1).eval(bindings).asBoolean();
          }
          IpAddressBlob blob = (IpAddressBlob) input.value();
          if (blob == null) {
            return ExprEval.ofType(ExpressionType.STRING, null);
          }
          return ExprEval.ofType(ExpressionType.STRING, blob.stringify(compact, false));
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
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }


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
            if (!TYPE.equals(input.type()) && input.value() != null) {
              throw new IAE("Function[%s] must take [%s] as input", name, TYPE.asTypeString());
            }
            IpAddressBlob blob = (IpAddressBlob) input.value();
            if (blob == null) {
              return ExprEval.ofComplex(TYPE, null);
            }
            return ExprEval.ofComplex(TYPE, blob.toPrefix(prefixLength));
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
            return TYPE;
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
          if (!TYPE.equals(input.type()) && input.value() != null) {
            throw new IAE("Function[%s] must take [%s] as input", name, TYPE.asTypeString());
          }
          ExprEval prefixSize = args.get(1).eval(bindings);
          IpAddressBlob blob = (IpAddressBlob) input.value();
          if (blob == null) {
            return ExprEval.ofComplex(TYPE, null);
          }
          int prefixLength = prefixSize.asInt();
          return ExprEval.ofComplex(TYPE, blob.toPrefix(prefixLength));
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
          return TYPE;
        }
      }

      return new DynamicPrefixExpr(args);
    }
  }

  public static class MatchExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ip_match";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      if (args.get(1).isLiteral()) {
        final ExprEval literalEval = args.get(1).eval(InputBindings.nilBindings());
        if (!literalEval.type().is(ExprType.STRING)) {
          throw new IAE(
              "Function[%s] second argument must be [%s] as input, got [%s]",
              NAME,
              ExpressionType.STRING.asTypeString(),
              literalEval.type()
          );
        }
        final String literal = literalEval.asString();

        class MatchExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
        {
          public MatchExpr(List<Expr> args)
          {
            super(NAME, args);
          }

          @Override
          public ExprEval eval(ObjectBinding bindings)
          {
            ExprEval input = args.get(0).eval(bindings);
            if (!TYPE.equals(input.type()) && input.value() != null) {
              throw new IAE(
                  "Function[%s] first argument must be [%s] as input, got [%s]",
                  name,
                  TYPE.asTypeString(),
                  input.type()
              );
            }
            IpAddressBlob blob = (IpAddressBlob) input.value();
            if (blob == null) {
              return ExprEval.ofLongBoolean(literal == null);
            }
            return ExprEval.ofLongBoolean(blob.matches(literal));
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
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          ExprEval matchesInput = args.get(1).eval(bindings);
          if (!TYPE.equals(input.type()) && input.value() != null) {
            throw new IAE(
                "Function[%s] first argument must be [%s] as input, got [%s]",
                name,
                TYPE.asTypeString(),
                input.type()
            );
          }
          if (!matchesInput.type().is(ExprType.STRING)) {
            throw new IAE(
                "Function[%s] second argument must be [%s] as input, got [%s]",
                name,
                ExpressionType.STRING.asTypeString(),
                matchesInput.type()
            );
          }
          IpAddressBlob blob = (IpAddressBlob) input.value();
          if (blob == null) {
            return ExprEval.ofLongBoolean(matchesInput.value() == null);
          }

          return ExprEval.ofLongBoolean(blob.matches(matchesInput.asString()));
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
    public BaseParseExpr(String name, Expr arg)
    {
      super(name, arg);
    }

    ExprEval parse(ObjectBinding bindings, boolean reportParseException)
    {
      ExprEval toParse = arg.eval(bindings);
      if (!toParse.type().is(ExprType.STRING)) {
        throw new IAE("Function[%s] must take a string as input, given [%s]", name, toParse.type().asTypeString());
      }
      IpAddressBlob blob = IpAddressBlob.parse(toParse.asString(), reportParseException);
      return ExprEval.ofComplex(TYPE, blob);
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return TYPE;
    }
  }
}
