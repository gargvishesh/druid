/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.fastrack;

import com.google.common.collect.ImmutableMap;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class UserAgentExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "ft_useragent";
  private static final UserAgentStringParser PARSER = UADetectorServiceFactory.getResourceModuleParser();
  private static final Map<String, Function<ReadableUserAgent, String>> FUNCTIONS =
      ImmutableMap.<String, Function<ReadableUserAgent, String>>builder()
          .put("browser", ReadableUserAgent::getName)
          .put("browser_version", agent -> agent.getVersionNumber().toVersionString())
          .put("agent_type", ReadableUserAgent::getTypeName)
          .put("agent_category", agent -> agent.getDeviceCategory().getName())
          .put("os", agent -> agent.getOperatingSystem().getName())
          .put("platform", agent -> agent.getOperatingSystem().getFamily().getName())
          .build();

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 2) {
      throw new IAE("Function[%s] must have 2 arguments", name());
    }

    final Expr arg = args.get(0);
    final String fnName = ((String) args.get(1).getLiteralValue());

    final Function<ReadableUserAgent, String> fn = FUNCTIONS.get(fnName);
    if (fn == null) {
      throw new IAE("Function[%s] cannot do[%s]", fnName);
    }

    class UserAgentExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private UserAgentExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        final String s = eval.asString();
        if (s != null) {
          final ReadableUserAgent agent = PARSER.parse(s);
          if (agent != null) {
            final String value = fn.apply(agent);
            if (!"unknown".equals(value)) {
              return ExprEval.of(value);
            }
          }
        }
        return ExprEval.of(null);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new UserAgentExpr(newArg));
      }
    }

    return new UserAgentExpr(arg);
  }
}
