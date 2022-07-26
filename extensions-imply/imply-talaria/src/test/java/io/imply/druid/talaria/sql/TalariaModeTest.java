/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.indexing.error.TalariaWarnings;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static io.imply.druid.talaria.sql.TalariaMode.CTX_TALARIA_MODE;
import static io.imply.druid.talaria.sql.TalariaMode.CTX_TALARIA_MODE_LEGACY_ALIASES;

public class TalariaModeTest
{

  @Test
  public void testPopulateQueryContextWhenNoSupercedingValuePresent()
  {
    QueryContext originalQueryContext = new QueryContext();
    TalariaMode.populateDefaultQueryContext("strict", originalQueryContext);
    Assert.assertEquals(ImmutableMap.of(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0), originalQueryContext.getMergedParams());
  }

  @Test
  public void testPopulateQueryContextWhenSupercedingValuePresent()
  {
    QueryContext originalQueryContext = new QueryContext(ImmutableMap.of(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10));
    TalariaMode.populateDefaultQueryContext("strict", originalQueryContext);
    Assert.assertEquals(ImmutableMap.of(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10), originalQueryContext.getMergedParams());

  }

  @Test
  public void testPopulateQueryContextWhenInvalidMode()
  {
    QueryContext originalQueryContext = new QueryContext();
    Assert.assertThrows(ISE.class, () -> {
      TalariaMode.populateDefaultQueryContext("fake_mode", originalQueryContext);
    });
  }

  @Test
  public void getTalariaModeNoParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals("strict", TalariaMode.getTalariaMode(new QueryContext()));
  }

  @Test
  public void getTalariaModeParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_TALARIA_MODE, "nonStrict");
    Assert.assertEquals("nonStrict", TalariaMode.getTalariaMode(new QueryContext(propertyMap)));
  }

  @Test
  public void getTalariaModeLegacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_TALARIA_MODE_LEGACY_ALIASES.get(0), "nonStrict");
    Assert.assertEquals("nonStrict", TalariaMode.getTalariaMode(new QueryContext(propertyMap)));
  }

  @Test
  public void getTalariaModeBothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_TALARIA_MODE, "nonStrict",
        CTX_TALARIA_MODE_LEGACY_ALIASES.get(0), "strict");
    Assert.assertEquals("nonStrict", TalariaMode.getTalariaMode(new QueryContext(propertyMap)));
  }
}
