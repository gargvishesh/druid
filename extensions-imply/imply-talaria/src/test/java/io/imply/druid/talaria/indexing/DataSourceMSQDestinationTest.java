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

public class DataSourceMSQDestinationTest
{

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DataSourceMSQDestination.class)
                  .withNonnullFields("dataSource", "segmentGranularity", "segmentSortOrder")
                  .usingGetClass()
                  .verify();
  }
}
