/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContext;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static io.imply.druid.talaria.indexing.TalariaInsertContextKeys.CTX_SORT_ORDER;
import static io.imply.druid.talaria.indexing.TalariaInsertContextKeys.CTX_SORT_ORDER_LEGACY_ALIASES;

public class TalariaInsertContextKeysTest
{
  @Test
  public void testDecodeSortOrder()
  {
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder("a, b,\"c,d\""));
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder(" a, b,\"c,d\""));
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder("[\"a\", \"b\", \"c,d\"]"));
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder(" [\"a\", \"b\", \"c,d\"] "));
    Assert.assertEquals(ImmutableList.of(), decodeSortOrder("[]"));
    Assert.assertEquals(ImmutableList.of(), decodeSortOrder(""));
    Assert.assertEquals(ImmutableList.of(), decodeSortOrder(null));

    Assert.assertThrows(IllegalArgumentException.class, () -> decodeSortOrder("[["));
  }

  @Test
  public void getSortOrderNoParameterSetReturnsDefaultValue()
  {
    Assert.assertNull(TalariaInsertContextKeys.getSortOrder(new QueryContext()));
  }

  @Test
  public void getSortOrderParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_SORT_ORDER, "a, b,\"c,d\"");
    Assert.assertEquals("a, b,\"c,d\"", TalariaInsertContextKeys.getSortOrder(new QueryContext(propertyMap)));
  }

  @Test
  public void getSortOrderLegacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_SORT_ORDER_LEGACY_ALIASES.get(0), "a, b,\"c,d\"");
    Assert.assertEquals("a, b,\"c,d\"", TalariaInsertContextKeys.getSortOrder(new QueryContext(propertyMap)));
  }

  @Test
  public void getSortOrderBothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_SORT_ORDER, "",
        CTX_SORT_ORDER_LEGACY_ALIASES.get(0), "a, b,\"c,d\"");
    Assert.assertEquals("", TalariaInsertContextKeys.getSortOrder(new QueryContext(propertyMap)));
  }

  private static List<String> decodeSortOrder(@Nullable final String input)
  {
    return TalariaInsertContextKeys.decodeSortOrder(input);
  }
}
