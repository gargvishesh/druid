/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.currency;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.UtilityBeltModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CurrencySumSerdeTest
{
  static final DateTime DAY1 = DateTimes.of("2000-01-01T00:00:00.000Z");
  static final DateTime DAY2 = DateTimes.of("2000-01-02T00:00:00.000Z");
  static final DateTime DAY3 = DateTimes.of("2000-01-03T00:00:00.000Z");
  static final Map<DateTime, Double> CONVERSIONS = ImmutableMap.of(
      DAY1, 2.0,
      DAY2, 3.0,
      DAY3, 4.0
  );
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    for (Module module : new UtilityBeltModule().getJacksonModules()) {
      MAPPER.registerModule(module);
    }
  }

  @Test
  public void testSerde() throws Exception
  {
    final CurrencySumAggregatorFactory factory = new CurrencySumAggregatorFactory(
        "eur",
        "usd",
        CONVERSIONS
    );

    Assert.assertEquals(
        factory,
        MAPPER.readValue(MAPPER.writeValueAsBytes(factory), AggregatorFactory.class)
    );
  }

  @Test
  public void testDeserialization() throws Exception
  {
    final String json = "{\n"
                        + "      \"type\" : \"currencySum\",\n"
                        + "      \"name\" : \"eur\",\n"
                        + "      \"fieldName\" : \"usd\",\n"
                        + "      \"conversions\": {\n"
                        + "        \"2000-01-01\" : 2.0,\n"
                        + "        \"2000-01-02\" : 3.0,\n"
                        + "        \"2000-01-03\" : 4.0\n"
                        + "      }\n"
                        + "    }";

    final CurrencySumAggregatorFactory factory = new CurrencySumAggregatorFactory(
        "eur",
        "usd",
        CONVERSIONS
    );

    Assert.assertEquals(
        factory,
        MAPPER.readValue(json, AggregatorFactory.class)
    );
  }
}
