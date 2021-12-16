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
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import io.imply.druid.tests.ImplyTestNGGroup;
import io.imply.druid.tests.client.AsyncResourceTestClient;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups = ImplyTestNGGroup.ASYNC_DOWNLOAD)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAsyncQueryLimits extends AbstractIndexerTest
{
  @Inject
  private AsyncResourceTestClient asyncResourceTestClient;

  @Test
  public void testClusterAsyncQueryRunningLimits() throws Exception
  {
    // This test covers the following limits:
    // 1. Cluster Concurrent Async Query Limit - number of concurrent Async queries per cluster. New queries will be queued after the cluster hits the limit.
    // 2. Queue limit. New queries will be rejected.
    List<String> asyncResultIds = new ArrayList<>();
    try {
      final SqlQuery query = new SqlQuery(
          "SELECT sleep(3), 10",
          ResultFormat.ARRAY,
          false,
          false,
          false,
          null,
          null
      );
      int maxQueryCanSubmit = asyncResourceTestClient.getMaxQueriesToQueue() + asyncResourceTestClient.getMaxConcurrentQueries();
      for (int i = 0; i < maxQueryCanSubmit; i++) {
        SqlAsyncQueryDetailsApiResponse response = asyncResourceTestClient.submitAsyncQuery(query);
        Assert.assertEquals(response.getState(), SqlAsyncQueryDetails.State.INITIALIZED);
        asyncResultIds.add(response.getAsyncResultId());
      }
      // The first asyncQueryLimitsConfig.getMaxConcurrentQueries() query submitted will be RUNNING
      for (int i = 0; i < asyncResourceTestClient.getMaxConcurrentQueries(); i++) {
        final String asyncResultId = asyncResultIds.get(i);
        ITRetryUtil.retryUntilTrue(
            () -> {
              SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
              return statusResponse.getState() == SqlAsyncQueryDetails.State.RUNNING;
            },
            "Waiting for async task to be running"
        );
      }
      // The remaining query submitted will still be in INITIALIZED state (queued)
      for (int i = asyncResourceTestClient.getMaxConcurrentQueries(); i < maxQueryCanSubmit; i++) {
        SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultIds.get(i));
        Assert.assertEquals(statusResponse.getState(), SqlAsyncQueryDetails.State.INITIALIZED);
      }

      // Now try submit exceeding queue limit. We should get error back since the queue is full
      try {
        asyncResourceTestClient.submitAsyncQuery(query);
        Assert.fail();
      }
      catch (QueryCapacityExceededException e) {
      }
    }
    finally {
      // Wait for all the query to be completed and cleanup
      for (String asyncResultId : asyncResultIds) {
        //TODO: Use the cancel API instead of waiting for cleanup
        ITRetryUtil.retryUntilTrue(
            () -> {
              SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
              return statusResponse == null;
            },
            "Wating for async cleanup coordinator duty to complete"
        );
      }
    }
  }
}
