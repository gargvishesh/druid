/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import org.junit.Assert;
import org.junit.Test;

public class SqlAsyncUtilTest
{
  @Test
  public void testCreateAsyncResultIdThenGetBrokerIdFromAsyncResultId()
  {
    String brokerId = "brokerid123";
    String queryId = "queryId567";
    String asyncResultId = SqlAsyncUtil.createAsyncResultId(brokerId, queryId);
    Assert.assertNotNull(asyncResultId);
    String brokerIdActual = SqlAsyncUtil.getBrokerIdFromAsyncResultId(asyncResultId);
    Assert.assertNotNull(brokerIdActual);
    Assert.assertEquals(brokerId, brokerIdActual);
  }

  @Test
  public void testCreateAsyncResultIdStartsWithBrokerIdFollowBySeparator()
  {
    String brokerId = "brokerid123";
    String queryId = "queryId567";
    String asyncResultId = SqlAsyncUtil.createAsyncResultId(brokerId, queryId);
    Assert.assertNotNull(asyncResultId);
    Assert.assertTrue(asyncResultId.startsWith(brokerId + SqlAsyncUtil.BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_STRING));
  }
}
