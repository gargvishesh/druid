/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.expressions;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class NestedDataExpressionsTest extends InitializedNullHandlingTest
{
  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(
          new NestedDataExpressions.StructExprMacro(),
          new NestedDataExpressions.GetPathExprMacro(),
          new NestedDataExpressions.ListKeysExprMacro(),
          new NestedDataExpressions.ListPathsExprMacro()
      )
  );
  private static final Map<String, Object> NEST = ImmutableMap.of(
      "x", 100L,
      "y", 200L,
      "z", 300L
  );

  private static final Map<String, Object> NESTER = ImmutableMap.of(
      "x", ImmutableList.of("a", "b", "c"),
      "y", ImmutableMap.of("a", "hello", "b", "world")
  );

  Expr.ObjectBinding inputBindings = InputBindings.withTypedSuppliers(
      new ImmutableMap.Builder<String, Pair<ExpressionType, Supplier<Object>>>()
          .put("nest", new Pair<>(NestedDataExpressions.TYPE, () -> NEST))
          .put("nester", new Pair<>(NestedDataExpressions.TYPE, () -> NESTER))
          .put("string", new Pair<>(ExpressionType.STRING, () -> "abcdef"))
          .put("long", new Pair<>(ExpressionType.LONG, () -> 1234L))
          .put("double", new Pair<>(ExpressionType.DOUBLE, () -> 1.234))
          .put("nullString", new Pair<>(ExpressionType.STRING, () -> null))
          .put("nullLong", new Pair<>(ExpressionType.LONG, () -> null))
          .put("nullDouble", new Pair<>(ExpressionType.DOUBLE, () -> null))
          .build()
  );

  @Test
  public void testStructExpression()
  {
    Expr expr = Parser.parse("struct('x',100,'y',200,'z',300)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(NEST, eval.value());

    expr = Parser.parse("struct('x',array('a','b','c'),'y',struct('a','hello','b','world'))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    // decompose because of array equals
    Assert.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) ((Map) eval.value()).get("x"));
    Assert.assertEquals(ImmutableMap.of("a", "hello", "b", "world"), ((Map) eval.value()).get("y"));
  }

  @Test
  public void testListKeysExpression()
  {
    Expr expr = Parser.parse("list_keys(nest, '.')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"x", "y", "z"}, (Object[]) eval.value());


    expr = Parser.parse("list_keys(nester, '.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"0", "1", "2"}, (Object[]) eval.value());

    expr = Parser.parse("list_keys(nester, '.y')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"a", "b"}, (Object[]) eval.value());

    expr = Parser.parse("list_keys(nester, '.x.a')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("list_keys(nester, '.x.a.b')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());
  }

  @Test
  public void testListPathsExpression()
  {
    Expr expr = Parser.parse("list_paths(nest)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{".\"x\"", ".\"y\"", ".\"z\""}, (Object[]) eval.value());

    expr = Parser.parse("list_paths(nester)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{".\"x\"[0]", ".\"x\"[1]", ".\"x\"[2]", ".\"y\".\"a\"", ".\"y\".\"b\""}, (Object[]) eval.value());

  }

  @Test
  public void testGetPathExpression()
  {
    Expr expr = Parser.parse("get_path(nest, '.x')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(100L, eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());

    expr = Parser.parse("get_path(nester, '.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("get_path(nester, '.x[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("b", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("get_path(nester, '.x[23]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("get_path(nester, '.x[1].b')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("get_path(nester, '.y[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("get_path(nester, '.y.a')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("hello", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("get_path(nester, '.y.a.b.c[12]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());
  }
}
