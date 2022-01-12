/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.expressions;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.expression.ExprUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class SessionizeExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String NAME = "SESSIONIZE";

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

    class SessionizeExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private SessionizeExpr(Expr arg)
      {
        super(NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        throw new UOE("%s evaluation not supported", NAME);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new SessionizeExpr(newArg));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }
    }

    return new SessionizeExpr(arg);
  }
}
