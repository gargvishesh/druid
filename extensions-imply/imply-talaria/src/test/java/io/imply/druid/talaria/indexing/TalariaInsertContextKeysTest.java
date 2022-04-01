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
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

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

  private static List<String> decodeSortOrder(@Nullable final String input)
  {
    return TalariaInsertContextKeys.decodeSortOrder(input);
  }
}
