/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class ServiceLocationTest
{
  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ServiceLocation.class)
                  .usingGetClass()
                  .verify();
  }
}
