/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TalariaQueryMakerTest
{
  @Test
  public void testValidateSegmentSortOrder()
  {
    // These are all OK, so validateSegmentSortOrder does nothing.
    TalariaQueryMaker.validateSegmentSortOrder(Collections.emptyList(), ImmutableList.of("__time", "a", "b"));
    TalariaQueryMaker.validateSegmentSortOrder(ImmutableList.of("__time"), ImmutableList.of("__time", "a", "b"));
    TalariaQueryMaker.validateSegmentSortOrder(ImmutableList.of("__time", "b"), ImmutableList.of("__time", "a", "b"));
    TalariaQueryMaker.validateSegmentSortOrder(ImmutableList.of("b"), ImmutableList.of("a", "b"));

    // These are not OK.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> TalariaQueryMaker.validateSegmentSortOrder(ImmutableList.of("c"), ImmutableList.of("a", "b"))
    );

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> TalariaQueryMaker.validateSegmentSortOrder(
            ImmutableList.of("b", "__time"),
            ImmutableList.of("__time", "a", "b")
        )
    );
  }
}
