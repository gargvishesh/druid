/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class TalariaCountersTest
{

  @Test
  public void testStateString()
  {
    TalariaCounters target = new TalariaCounters();
    target.getOrCreateChannelCounters(MSQCounterType.INPUT_DRUID, 1, 1, 1);
    target.getOrCreateChannelCounters(MSQCounterType.INPUT_DRUID, 1, 2, 2);
    Assert.assertEquals("S1:W1:0; S2:W1:0", target.stateString());
  }

  @Test
  public void testStagePartitionEquals()
  {
    EqualsVerifier.forClass(TalariaCounters.StagePartitionNumber.class)
                  .usingGetClass()
                  .verify();
  }
}
