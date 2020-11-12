/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.fastrack;

import ch.hsr.geohash.GeoHash;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.google.inject.Inject;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import io.imply.druid.UtilityBeltConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GeoIpExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "ft_geoip";

  private static final Map<String, Function<CityResponse, Object>> FUNCTIONS =
      ImmutableMap.<String, Function<CityResponse, Object>>builder()
          .put(
              "lat",
              geo -> geo.getLocation() != null && geo.getLocation().getLatitude() != null
                     ? geo.getLocation().getLatitude()
                     : null
          )
          .put(
              "lon",
              geo -> geo.getLocation() != null && geo.getLocation().getLongitude() != null
                     ? geo.getLocation().getLongitude()
                     : null
          )
          .put("geohash9", geo -> {
            if (geo.getLocation().getLatitude() != null && geo.getLocation().getLongitude() != null) {
              return GeoHash.geoHashStringWithCharacterPrecision(
                  geo.getLocation().getLatitude(),
                  geo.getLocation().getLongitude(),
                  9
              );
            } else {
              return null;
            }
          })
          .put("city", geo -> geo.getCity().getName())
          .put("metro", geo -> geo.getLocation().getMetroCode())
          .put("region", geo -> geo.getMostSpecificSubdivision().getName())
          .put("regionIso", geo -> geo.getMostSpecificSubdivision().getIsoCode())
          .put("country", geo -> geo.getCountry().getName())
          .put("countryIso", geo -> geo.getCountry().getIsoCode())
          .put("continent", geo -> geo.getContinent().getName())
          .build();

  private final DatabaseReader geoReader;

  @Inject
  public GeoIpExprMacro(final UtilityBeltConfig config)
  {
    if (config.getGeoDatabaseFile() == null) {
      geoReader = null;
    } else {
      try {
        geoReader = new DatabaseReader.Builder(config.getGeoDatabaseFile())
            .fileMode(Reader.FileMode.MEMORY_MAPPED)
            .build();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    if (geoReader != null) {
      geoReader.close();
    }
  }

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

    final Function<CityResponse, Object> fn = FUNCTIONS.get(fnName);
    if (fn == null) {
      throw new IAE("Function[%s] cannot do[%s]", fnName);
    }

    class GeoIpExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      GeoIpExpr(Expr arg)
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
          final CityResponse response = lookup(s);
          if (response != null) {
            return ExprEval.bestEffortOf(fn.apply(response));
          }
        }

        return ExprEval.of(null);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArgs = shuttle.visit(arg);
        return shuttle.visit(new GeoIpExpr(newArgs));
      }
    }

    return new GeoIpExpr(arg);
  }

  @Nullable
  private CityResponse lookup(@Nullable final String addressString)
  {
    if (addressString == null) {
      return null;
    }

    final InetAddress inetAddress;
    try {
      inetAddress = InetAddresses.forString(addressString);
    }
    catch (IllegalArgumentException e) {
      return null;
    }

    try {
      return geoReader.city(inetAddress);
    }
    catch (AddressNotFoundException e) {
      return null;
    }
    catch (Exception e) {
      throw new RE(StringUtils.format("Could not look up geo data for[%s]", addressString), e);
    }
  }
}
