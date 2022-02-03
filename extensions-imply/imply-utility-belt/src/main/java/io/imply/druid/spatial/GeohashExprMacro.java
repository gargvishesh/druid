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

import ch.hsr.geohash.GeoHash;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import java.util.List;
import java.util.stream.Collectors;

public class GeohashExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "st_geohash";
  private static final double MIN_LONGITUDE = -180;
  private static final double MAX_LONGITUDE = 180;
  private static final double MIN_LATITUDE = -90;
  private static final double MAX_LATITUDE = 90;

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 3) {
      throw new IAE("Function[%s] must have 3 arguments", name());
    }

    class GeohashExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final int maxChars;

      public GeohashExpr(final List<Expr> args)
      {
        super(FN_NAME, args);

        final Expr maxCharsExpr = args.get(2);

        if (!maxCharsExpr.isLiteral() || !(maxCharsExpr.getLiteralValue() instanceof Number)) {
          throwInvalidMaxCharsException();
        }

        maxChars = ((Number) maxCharsExpr.getLiteralValue()).intValue();

        if (maxChars < 1 || maxChars > 12) {
          throwInvalidMaxCharsException();
        }
      }

      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        final ExprEval lonEval = args.get(0).eval(bindings);
        if (lonEval.isNumericNull()) {
          return ExprEval.of(null);
        }

        final ExprEval latEval = args.get(1).eval(bindings);
        if (latEval.isNumericNull()) {
          return ExprEval.of(null);
        }

        final double lon = lonEval.asDouble();

        if (lon <= MIN_LONGITUDE || lon > MAX_LONGITUDE) {
          return ExprEval.of(null);
        }

        final double lat = latEval.asDouble();

        if (lat <= MIN_LATITUDE || lat > MAX_LATITUDE) {
          return ExprEval.of(null);
        }

        final String geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, maxChars);
        return ExprEval.of(geohash);
      }

      @Override
      public Expr visit(final Shuttle shuttle)
      {
        List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
        return shuttle.visit(new GeohashExpr(newArgs));
      }

      private void throwInvalidMaxCharsException()
      {
        throw new IAE(
            "Function[%s] argument \"maxchars\" must be a numeric literal between 1 and 12 (inclusive)",
            name()
        );
      }
    }

    return new GeohashExpr(args);
  }
}
