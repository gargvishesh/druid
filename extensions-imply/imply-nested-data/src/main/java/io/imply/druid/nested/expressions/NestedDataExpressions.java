/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.expressions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.nested.column.NestedDataComplexTypeSerde;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class NestedDataExpressions
{
  public static final ExpressionType TYPE = Preconditions.checkNotNull(
      ExpressionType.fromColumnType(NestedDataComplexTypeSerde.TYPE)
  );

  public static class StructExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "struct";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      Preconditions.checkArgument(args.size() % 2 == 0);
      class StructExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public StructExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ImmutableMap.Builder<String, Object> bob = new ImmutableMap.Builder<>();
          for (int i = 0; i < args.size(); i += 2) {
            ExprEval field = args.get(i).eval(bindings);
            ExprEval value = args.get(i + 1).eval(bindings);

            Preconditions.checkArgument(field.type().is(ExprType.STRING), "struct field name must be a STRING");
            bob.put(field.asString(), value.value());
          }

          return ExprEval.ofType(TYPE, bob.build());
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new StructExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return TYPE;
        }
      }
      return new StructExpr(args);
    }
  }

  public static class GetPathExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "get_path";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class GetPathExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {

        public GetPathExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return ExprEval.of(null);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new GetPathExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING;
        }
      }
      return new GetPathExpr(args);
    }
  }
}
