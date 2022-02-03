/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.spatial;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GeohashExprMacroTest extends InitializedNullHandlingTest
{
  private ExprMacroTable macroTable;

  @Before
  public void setUp()
  {
    macroTable = new ExprMacroTable(Collections.singletonList(new GeohashExprMacro()));
  }

  @Test
  public void testEvaluateFromLiterals()
  {
    final Expr expr = Parser.parse("st_geohash(-122.431297, 37.773972, 12)", macroTable);
    Assert.assertEquals("9q8yyh83248p", evaluate(expr, null, null));
  }

  @Test
  public void testEvaluateFromBindings()
  {
    final Expr expr = Parser.parse("st_geohash(lon, lat, 12)", macroTable);
    Assert.assertEquals("9q8yyh83248p", evaluate(expr, -122.431297, 37.773972));
  }

  @Test
  public void testEvaluateFromStringBindings()
  {
    final Expr expr = Parser.parse("st_geohash(lon, lat, 12)", macroTable);
    Assert.assertEquals("9q8yyh83248p", evaluate(expr, "-122.431297", "37.773972"));
  }

  @Test
  public void testEvaluateFromOutOfRangeLongitudeBinding()
  {
    final Expr expr = Parser.parse("st_geohash(lon, lat, 12)", macroTable);
    Assert.assertNull(evaluate(expr, -192.431297, 37.773972));
  }

  @Test
  public void testEvaluateFromOutOfRangeLatitudeBinding()
  {
    final Expr expr = Parser.parse("st_geohash(lon, lat, 12)", macroTable);
    Assert.assertNull(evaluate(expr, -122.431297, 137.773972));
  }

  @Test
  public void testEvaluateFromNullLongitudeBinding()
  {
    final Expr expr = Parser.parse("st_geohash(lon, lat, 12)", macroTable);
    Assert.assertNull(evaluate(expr, null, 37.773972));
  }

  @Test
  public void testEvaluateFromNullLatitudeBinding()
  {
    final Expr expr = Parser.parse("st_geohash(lon, lat, 12)", macroTable);
    Assert.assertNull(evaluate(expr, -122.431297, null));
  }

  @Test
  public void testEvaluateMaxCharsTooLarge()
  {
    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> Parser.parse("st_geohash(lon, lat, 13)", macroTable)
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("\"maxchars\" must be a numeric literal between 1 and 12")
        )
    );
  }

  private static String evaluate(final Expr expr, final Object lon, final Object lat)
  {
    final Map<String, Object> map = new HashMap<>();
    map.put("lon", lon);
    map.put("lat", lat);
    return expr.eval(InputBindings.withMap(map)).asString();
  }
}
