/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.fastrack;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UserAgentExprMacroTest extends InitializedNullHandlingTest
{
  private static final String BROWSER = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36";

  private ExprMacroTable macroTable;

  @Before
  public void setUp()
  {
    macroTable = new ExprMacroTable(Collections.singletonList(new UserAgentExprMacro()));
  }

  @Test
  public void testBrowser()
  {
    final Expr expr = Parser.parse("ft_useragent(ua, 'browser')", macroTable);
    Assert.assertEquals("Chrome", evaluate(expr, BROWSER));
    Assert.assertNull(evaluate(expr, "foo"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testBrowserVersion()
  {
    final Expr expr = Parser.parse("ft_useragent(ua, 'browser_version')", macroTable);
    Assert.assertEquals("70.0.3538.77", evaluate(expr, BROWSER));
    Assert.assertEquals(NullHandling.emptyToNullIfNeeded(""), evaluate(expr, "foo"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testAgentType()
  {
    final Expr expr = Parser.parse("ft_useragent(ua, 'agent_type')", macroTable);
    Assert.assertEquals("Browser", evaluate(expr, BROWSER));
    Assert.assertEquals(NullHandling.emptyToNullIfNeeded(""), evaluate(expr, "foo"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testAgentCategory()
  {
    final Expr expr = Parser.parse("ft_useragent(ua, 'agent_category')", macroTable);
    Assert.assertEquals("Personal computer", evaluate(expr, BROWSER));
    Assert.assertEquals(NullHandling.emptyToNullIfNeeded(""), evaluate(expr, "foo"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testOs()
  {
    final Expr expr = Parser.parse("ft_useragent(ua, 'os')", macroTable);
    Assert.assertEquals("OS X", evaluate(expr, BROWSER));
    Assert.assertNull(evaluate(expr, "foo"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testPlatform()
  {
    final Expr expr = Parser.parse("ft_useragent(ua, 'platform')", macroTable);
    Assert.assertEquals("OS X", evaluate(expr, BROWSER));
    Assert.assertEquals(NullHandling.emptyToNullIfNeeded(""), evaluate(expr, "foo"));
    Assert.assertNull(evaluate(expr, null));

  }

  private static String evaluate(final Expr expr, final String addr)
  {
    final Map<String, Object> map = new HashMap<>();
    map.put("ua", addr);
    return expr.eval(InputBindings.withMap(map)).asString();
  }

}
