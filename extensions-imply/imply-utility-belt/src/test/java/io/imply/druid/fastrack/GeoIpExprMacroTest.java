/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.fastrack;

import com.google.common.base.Preconditions;
import io.imply.druid.UtilityBeltConfig;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GeoIpExprMacroTest extends InitializedNullHandlingTest
{
  private GeoIpExprMacro exprMacro;
  private ExprMacroTable macroTable;

  @Before
  public void setUp()
  {
    final UtilityBeltConfig config = new UtilityBeltConfig();
    final URL testDbURL = Preconditions.checkNotNull(
        getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb"),
        "Can't find test DB?!"
    );
    config.geoDatabase = new File(testDbURL.getFile());

    exprMacro = new GeoIpExprMacro(config);
    macroTable = new ExprMacroTable(Collections.singletonList(exprMacro));
  }

  @After
  public void tearDown() throws Exception
  {
    exprMacro.stop();
  }

  @Test
  public void testRegion()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'region')", macroTable);
    Assert.assertEquals("Östergötland County", evaluate(expr, "89.160.20.113"));
    Assert.assertNull(evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testRegionCode()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'regionIso')", macroTable);
    Assert.assertEquals("E", evaluate(expr, "89.160.20.113"));
    Assert.assertNull(evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testCountry()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'country')", macroTable);
    Assert.assertEquals("Sweden", evaluate(expr, "89.160.20.113"));
    Assert.assertEquals("Philippines", evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testCountryCode()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'countryIso')", macroTable);
    Assert.assertEquals("SE", evaluate(expr, "89.160.20.113"));
    Assert.assertEquals("PH", evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testContinent()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'continent')", macroTable);
    Assert.assertEquals("Europe", evaluate(expr, "89.160.20.113"));
    Assert.assertEquals("Asia", evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testMetro()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'metro')", macroTable);
    Assert.assertEquals("819", evaluate(expr, "216.160.83.56"));
    Assert.assertNull(evaluate(expr, "89.160.20.113"));
    Assert.assertNull(evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testCity()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'city')", macroTable);
    Assert.assertEquals("Linköping", evaluate(expr, "89.160.20.113"));
    Assert.assertNull(evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  @Test
  public void testGeohash9()
  {
    final Expr expr = Parser.parse("ft_geoip(addr, 'geohash9')", macroTable);
    Assert.assertEquals("u67h767tz", evaluate(expr, "89.160.20.113"));
    Assert.assertEquals("wdqcbntdq", evaluate(expr, "202.196.224.1"));
    Assert.assertNull(evaluate(expr, "127.0.0.1"));
    Assert.assertNull(evaluate(expr, null));
  }

  private static String evaluate(final Expr expr, final String addr)
  {
    final Map<String, Object> map = new HashMap<>();
    map.put("addr", addr);
    return expr.eval(Parser.withMap(map)).asString();
  }
}
