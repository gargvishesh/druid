/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.expression;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import io.imply.druid.inet.column.IpAddressBlob;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IpAddressExpressionsTest extends InitializedNullHandlingTest
{
  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(
          new IpAddressExpressions.ParseExprMacro(),
          new IpAddressExpressions.TryParseExprMacro(),
          new IpAddressExpressions.StringifyExprMacro(),
          new IpAddressExpressions.PrefixExprMacro(),
          new IpAddressExpressions.MatchExprMacro()
      )
  );
  private static final String V4_STRING = "1.2.3.4";
  private static final String V6_STRING = "2001:0db8:0000:0000:0000:8a2e:0370:7334";
  private static final String V6_COMPACT;
  private static final String CIDR_V6 = "2001::/16";
  private static final String CIDR_v4 = "1.2.0.0/16";

  static {
    try {
      V6_COMPACT = new IPAddressString(V6_STRING).toAddress().toIPv6().toCompressedString();
    }
    catch (AddressStringException e) {
      throw new RuntimeException("failed");
    }
  }

  private static final IpAddressBlob V4_BLOB = IpAddressBlob.ofString(V4_STRING);
  private static final IpAddressBlob V6_BLOB = IpAddressBlob.ofString(V6_STRING);

  Expr.ObjectBinding inputBindings = InputBindings.withTypedSuppliers(
      new ImmutableMap.Builder<String, Pair<ExpressionType, Supplier<Object>>>()
          .put("ipv4", new Pair<>(IpAddressExpressions.TYPE, () -> V4_BLOB))
          .put("ipv6", new Pair<>(IpAddressExpressions.TYPE, () -> V6_BLOB))
          .put("ipv4_string", new Pair<>(ExpressionType.STRING, () -> V4_STRING))
          .put("ipv6_string", new Pair<>(ExpressionType.STRING, () -> V6_STRING))
          .put("cidr_v4", new Pair<>(ExpressionType.STRING, () -> CIDR_v4))
          .put("cidr_v6", new Pair<>(ExpressionType.STRING, () -> CIDR_V6))
          .put("string", new Pair<>(ExpressionType.STRING, () -> "abcdef"))
          .put("long", new Pair<>(ExpressionType.LONG, () -> 1234L))
          .put("double", new Pair<>(ExpressionType.DOUBLE, () -> 1.234))
          .put("nullString", new Pair<>(ExpressionType.STRING, () -> null))
          .put("nullLong", new Pair<>(ExpressionType.LONG, () -> null))
          .put("nullDouble", new Pair<>(ExpressionType.DOUBLE, () -> null))
          .build()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParse()
  {
    Expr expr = Parser.parse("ip_parse(ipv4_string)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(V4_BLOB, eval.value());

    expr = Parser.parse("ip_parse(ipv6_string)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(V6_BLOB, eval.value());
  }

  @Test
  public void testParseException()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Cannot parse [abcdef] as an IP address");
    Expr expr = Parser.parse("ip_parse(string)", MACRO_TABLE);
    expr.eval(inputBindings);
  }

  @Test
  public void testParseBadArgs()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[ip_parse] must take a string as input, given [COMPLEX<ipAddress>]");
    Expr expr = Parser.parse("ip_parse(ipv4)", MACRO_TABLE);
    expr.eval(inputBindings);
  }

  @Test
  public void testTryParse()
  {
    Expr expr = Parser.parse("ip_try_parse(ipv4_string)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(V4_BLOB, eval.value());

    expr = Parser.parse("ip_try_parse(string)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("ip_try_parse(ipv6_string)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(V6_BLOB, eval.value());
  }

  @Test
  public void testStringify()
  {
    Expr expr = Parser.parse("ip_stringify(ipv4)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(V4_STRING, eval.value());

    expr = Parser.parse("ip_stringify(ipv6)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(V6_COMPACT, eval.value());

    expr = Parser.parse("ip_stringify(ipv6, 0)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(V6_STRING, eval.value());

    expr = Parser.parse("ip_stringify(ipv6, 1)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(V6_COMPACT, eval.value());

    expr = Parser.parse("ip_stringify(ip_try_parse(string))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("ip_stringify(ip_parse(ipv6_string))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(V6_COMPACT, eval.value());

  }

  @Test
  public void testPrefix()
  {
    final String sixPrefix16 = "2001::";
    Expr expr = Parser.parse("ip_prefix(ipv6, 16)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(IpAddressBlob.ofString(sixPrefix16), eval.value());

    expr = Parser.parse("ip_stringify(ip_prefix(ipv6, 16))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(sixPrefix16, eval.value());


    expr = Parser.parse("ip_stringify(ip_prefix(ipv6, 16), 0)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("2001:0000:0000:0000:0000:0000:0000:0000", eval.value());

    final String fourPrefix16 = "1.2.0.0";
    expr = Parser.parse("ip_prefix(ipv4, 16)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(IpAddressBlob.ofString(fourPrefix16), eval.value());

    expr = Parser.parse("ip_stringify(ip_prefix(ipv4, 16), 0)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(fourPrefix16, eval.value());
  }

  @Test
  public void testMatch()
  {
    Expr expr = Parser.parse("ip_match(ipv6, ipv6_string)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv6, '2001::/16')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv6, cidr_v6)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv6, '2002::/16')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertFalse(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv4, ipv4_string)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv4, '1.2.0.0/16')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv4, cidr_v4)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ipv6, '1.2.0.0/24')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertFalse(eval.asBoolean());


    expr = Parser.parse("ip_match(ip_parse('0.1.2.3'), '0.1.2.0/23')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ip_parse('0.1.2.3'), '0.1.2.0/22')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ip_parse('1:2:3:4:5:6:7:8'), '1:2:3:4:5::/80')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());

    expr = Parser.parse("ip_match(ip_parse('1:2:3:4:5:6:7:8'), '1:2:3:4:5::/70')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertTrue(eval.asBoolean());
  }

  @Test
  public void testMatchBadRange()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot match [2001:db8::8a2e:370:7334] with invalid address [NOT AN IP]");
    Expr expr = Parser.parse("ip_match(ipv6, 'NOT AN IP')", MACRO_TABLE);
    expr.eval(inputBindings);
  }

  @Test
  public void testNullHandlings()
  {
    Expr expr = Parser.parse("ip_match(nullString, ipv6_string)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertFalse(eval.asBoolean());

    expr = Parser.parse("ip_match(null, ipv6_string)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertFalse(eval.asBoolean());

    expr = Parser.parse("ip_stringify(nullString)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_stringify(null)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_parse(nullString)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_parse(null)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_try_parse(nullString)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_try_parse(null)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_prefix(nullString,20)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());

    expr = Parser.parse("ip_prefix(null,20)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
  }
}
