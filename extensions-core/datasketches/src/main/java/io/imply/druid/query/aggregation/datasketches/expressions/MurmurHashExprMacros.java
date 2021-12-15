/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.expressions;

import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.expression.ExprUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;

@EverythingIsNonnullByDefault
public class MurmurHashExprMacros
{
  public static class Murmur3Macro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ds_utf8_murmur3";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE(ExprUtils.createErrMsg(name(), "must have 1 argument"));
      }

      Expr arg = args.get(0);

      class Murmur3Expr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
      {
        private Murmur3Expr(Expr arg)
        {
          super(NAME, arg);
        }

        @Nonnull
        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          ExprEval eval = arg.eval(bindings);
          if (eval.asString() == null) {
            return ExprEval.of(null);
          }

          long[] hash = MurmurHash3.hash(eval.asString().getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED);
          byte[] byteHash = new byte[hash.length * Long.BYTES];
          int byteCounter = 0;
          for (long l : hash) { // does this and the nested loop get vectorized? only 2 longs. or should we inline?
            for (int idx = 0; idx < 8; idx++) {
              byteHash[byteCounter++] = (byte) (l >> (idx * 8) & 0xff);
            }
          }
          return ExprEval.of(new String(byteHash, StandardCharsets.UTF_8));
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new Murmur3Expr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING;
        }
      }

      return new Murmur3Expr(arg);
    }
  }

  public static class Murmur3_64Macro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "ds_utf8_murmur3_64";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE(ExprUtils.createErrMsg(name(), "must have 1 argument"));
      }

      Expr arg = args.get(0);

      class Murmur3Expr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
      {
        private Murmur3Expr(Expr arg)
        {
          super(NAME, arg);
        }

        @Nonnull
        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          ExprEval eval = arg.eval(bindings);
          return ExprEval.ofLong(eval.asString() == null ?
                             null :
                             MurmurHash3.hash(eval.asString().getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          Expr newArg = arg.visit(shuttle);
          return shuttle.visit(new Murmur3Expr(newArg));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING;
        }
      }

      return new Murmur3Expr(arg);
    }
  }
}
