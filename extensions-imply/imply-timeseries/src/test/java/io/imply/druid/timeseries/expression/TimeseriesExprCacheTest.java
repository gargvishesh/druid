/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import io.imply.druid.timeseries.Util;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TimeseriesExprCacheTest
{

  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testCacheKey()
  {
    // max over timeseries
    Assert.assertEquals(
        buildExprCacheKey("max_over_timeseries(\"ts\")"),
        getParsedExprCacheKey("max_over_timeseries(ts)")
    );

    // timeseries to json
    Assert.assertEquals(
        buildExprCacheKey("timeseries_to_json(\"ts\")"),
        getParsedExprCacheKey("timeseries_to_json(ts)")
    );

    // TWA
    Assert.assertEquals(
        buildExprCacheKey("time_weighted_average(\"ts\", 'linear', 'PT1H')"),
        getParsedExprCacheKey("time_weighted_average(ts, 'linear', 'PT1H')")
    );

    // delta
    Assert.assertEquals(
        buildExprCacheKey("delta_timeseries(\"ts\", 'PT1H')"),
        getParsedExprCacheKey("delta_timeseries(ts, 'PT1H')")
    );

    // IRR over timeseries
    Assert.assertEquals(
        buildExprCacheKey("irr(\"ts\", 100, 100, '2000-01-01T00:00:00Z\\/2000-01-03T00:00:00Z', 0.1)"),
        getParsedExprCacheKey("irr(ts, 100, 100, '2000-01-01T00:00:00Z/2000-01-03T00:00:00Z', 0.1)")
    );

    // IRR debug over timeseries
    Assert.assertEquals(
        buildExprCacheKey("irr_debug(\"ts\", 100, 100, '2000-01-01T00:00:00Z\\/2000-01-03T00:00:00Z', 0.1)"),
        getParsedExprCacheKey("irr_debug(ts, 100, 100, '2000-01-01T00:00:00Z/2000-01-03T00:00:00Z', 0.1)")
    );

    // timeseries size
    Assert.assertEquals(
        buildExprCacheKey("timeseries_size(\"ts\")"),
        getParsedExprCacheKey("timeseries_size(ts)")
    );

    // sum over timeseries
    Assert.assertEquals(
        buildExprCacheKey("sum_over_timeseries(\"ts\")"),
        getParsedExprCacheKey("sum_over_timeseries(ts)")
    );

    // quantile over timeseries
    Assert.assertEquals(
        buildExprCacheKey("quantile_over_timeseries(\"ts\", 0.95)"),
        getParsedExprCacheKey("quantile_over_timeseries(ts, 0.95)")
    );
  }

  private String getParsedExprCacheKey(String expr)
  {
    return new String(Parser.parse(
        expr,
        Util.makeTimeSeriesMacroTable()).getCacheKey(),
                      StandardCharsets.UTF_8
    );
  }

  private String buildExprCacheKey(String exprKeyPart)
  {
    return new StringBuilder().append('\0').appendCodePoint(7).append(exprKeyPart).toString();
  }
}
