/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.async;

import com.google.inject.Inject;
import io.imply.druid.sql.async.AsyncQueryPoolConfig;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import io.imply.druid.tests.ImplyTestNGGroup;
import io.imply.druid.tests.client.AsyncResourceTestClient;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DruidClusterAdminClient;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups = ImplyTestNGGroup.ASYNC_DOWNLOAD)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAsyncBrokerShutdown extends AbstractIndexerTest
{
  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  @Inject
  private AsyncResourceTestClient asyncResourceTestClient;

  @Test
  public void testQueryMarkFailedWhenBrokerShutdown() throws Exception
  {
    List<String> asyncResultIds = new ArrayList<>();
    try {
      final SqlQuery query = new SqlQuery(
          "SELECT sleep(6000), 10",
          ResultFormat.ARRAY,
          false,
          false,
          false,
          null,
          null
      );
      AsyncQueryPoolConfig asyncQueryLimitsConfig = asyncResourceTestClient.getAsyncQueryPoolConfig();
      int maxQueryCanSubmit = asyncQueryLimitsConfig.getMaxQueriesToQueue() + asyncQueryLimitsConfig.getMaxConcurrentQueries();
      // Submit queries so that we have both running queries and queued query
      for (int i = 0; i < maxQueryCanSubmit; i++) {
        SqlAsyncQueryDetailsApiResponse response = asyncResourceTestClient.submitAsyncQuery(query);
        Assert.assertEquals(response.getState(), SqlAsyncQueryDetails.State.INITIALIZED);
        asyncResultIds.add(response.getAsyncResultId());
      }

      // Restart the broker
      druidClusterAdminClient.restartBrokerContainer();

      // Wait until broker is ready again
      druidClusterAdminClient.waitUntilBrokerReady();

      // Verify that all earlier queries status are FAILED
      for (String asyncResultId : asyncResultIds) {
        SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
        Assert.assertEquals(statusResponse.getState(), SqlAsyncQueryDetails.State.FAILED);
        //We should be able to clean the query
        Assert.assertTrue(asyncResourceTestClient.cancel(asyncResultId));
        Assert.assertNull(asyncResourceTestClient.getStatus(asyncResultId));

      }
    }
    finally {
      // Wait for all the query to be completed and cleanup
      for (String asyncResultId : asyncResultIds) {
        asyncResourceTestClient.cancel(asyncResultId);
      }
    }
  }
}
