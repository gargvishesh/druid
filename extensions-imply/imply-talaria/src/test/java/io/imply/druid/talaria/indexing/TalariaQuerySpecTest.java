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
import org.junit.Test;

public class TalariaQuerySpecTest
{

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TalariaQuerySpec.class)
                  .withNonnullFields("query", "destination", "tuningConfig")
                  .usingGetClass()
                  .verify();
  }
}
