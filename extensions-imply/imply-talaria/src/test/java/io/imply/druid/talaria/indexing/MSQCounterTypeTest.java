/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MSQCounterTypeTest
{

  @Test
  public void testFromString()
  {
    assertEquals(MSQCounterType.INPUT_EXTERNAL, MSQCounterType.fromString("inputExternal"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromInvalidStringThrowsException()
  {
    assertEquals(MSQCounterType.INPUT_EXTERNAL, MSQCounterType.fromString("inptExt"));
  }
}
