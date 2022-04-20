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
import io.imply.druid.nested.column.PathFinder;
import io.imply.druid.nested.column.StructuredData;
import io.imply.druid.nested.column.StructuredDataProcessor;
import org.apache.druid.java.util.common.IAE;
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

            Preconditions.checkArgument(field.type().is(ExprType.STRING), "field name must be a STRING");
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
      final List<PathFinder.PathPartFinder> parts = getArg1PathPartsFromLiteral(NAME, args);
      class GetPathExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public GetPathExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          checkArg0NestedType(name, args, input);
          return ExprEval.bestEffortOf(
              PathFinder.findLiteral(maybeUnwrapStructuredData(input), parts)
          );
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
          // we cannot infer the output type (well we could say it is 'STRING' right now because is all we support...
          return null;
        }
      }
      return new GetPathExpr(args);
    }
  }

  public static class ListPathsExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "list_paths";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final StructuredDataProcessor processor = new StructuredDataProcessor()
      {
        @Override
        public int processLiteralField(String fieldName, Object fieldValue)
        {
          // do nothing, we only want the list of fields returned by this processor
          return 0;
        }
      };

      class ListPathsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ListPathsExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          checkArg0NestedType(name, args, input);
          StructuredDataProcessor.ProcessResults info = processor.processFields(maybeUnwrapStructuredData(input));
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              info.getLiteralFields()
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ListPathsExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING_ARRAY;
        }
      }
      return new ListPathsExpr(args);
    }
  }

  public static class ListKeysExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "list_keys";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final List<PathFinder.PathPartFinder> parts = getArg1PathPartsFromLiteral(NAME, args);
      class ListKeysExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ListKeysExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          checkArg0NestedType(name, args, input);
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              PathFinder.findKeys(maybeUnwrapStructuredData(input), parts)
          );
        }


        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ListKeysExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING_ARRAY;
        }
      }
      return new ListKeysExpr(args);
    }
  }

  @Nullable
  static Object maybeUnwrapStructuredData(ExprEval input)
  {
    if (input.value() instanceof StructuredData) {
      StructuredData data = (StructuredData) input.value();
      return data.getValue();
    }
    return input.value();
  }

  static void checkArg0NestedType(String fnName, List<Expr> args, ExprEval input)
  {
    if (!TYPE.equals(input.type()) && input.value() != null) {
      throw new IAE(
          "Function[%s] first argument [%s] must be type [%s] as input, got [%s]",
          fnName,
          args.get(0).stringify(),
          TYPE.asTypeString(),
          input.type().asTypeString()
      );
    }
  }

  static List<PathFinder.PathPartFinder> getArg1PathPartsFromLiteral(String fnName, List<Expr> args)
  {
    if (!(args.get(1).isLiteral() && args.get(1).getLiteralValue() instanceof String)) {
      throw new IAE(
          "Function[%s] second argument [%s] must be a literal [%s] value, got [%s] instead",
          fnName,
          args.get(1).stringify(),
          ExpressionType.STRING
      );
    }
    final List<PathFinder.PathPartFinder> parts = PathFinder.parseJqPath((String) args.get(1).getLiteralValue());
    return parts;
  }
}
