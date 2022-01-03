/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;


public class AsyncQueryConfigTest
{

  @Test
  public void testDefaults()
  {
    AsyncQueryConfig asyncQueryConfg = new AsyncQueryConfig(null, null, null);
    Assert.assertEquals(Duration.standardSeconds(10L), asyncQueryConfg.getReadRefreshTime());
    Assert.assertEquals(1, asyncQueryConfg.getMaxConcurrentQueries());
    Assert.assertEquals(10, asyncQueryConfg.getMaxQueriesToQueue());
  }

  @Test
  public void testReadRefreshTimeMinimums()
  {
    // tests just below the minimum
    try {
      new AsyncQueryConfig(
          null,
          null,
          Duration.standardSeconds(1L).minus(Duration.millis(1L))
      );
    }
    catch (Exception e) {
      // expected
    }
    // test the minimum allowed
    new AsyncQueryConfig(
        null,
        null,
        Duration.standardSeconds(1L)
    );
  }
}
