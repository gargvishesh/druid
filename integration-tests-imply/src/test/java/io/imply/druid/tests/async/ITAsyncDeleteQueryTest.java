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
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DruidClusterAdminClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.util.List;

@Test(groups = ImplyTestNGGroup.ASYNC_DOWNLOAD)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAsyncDeleteQueryTest extends AbstractIndexerTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";
  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  private AsyncResourceTestClient asyncResourceTestClient;

  private String fullDatasourceName;

  @BeforeMethod
  public void setFullDatasourceName(Method method)
  {
    fullDatasourceName = INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix() + "-" + method.getName();
  }

  @Test
  public void testKillAsyncQuerySanity() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      loadData(INDEX_TASK, fullDatasourceName);
      final SqlQuery query = new SqlQuery(
          "SELECT count(*) FROM \"" + fullDatasourceName + "\"",
          ResultFormat.ARRAY,
          false,
          false,
          false,
          null,
          null
      );
      SqlAsyncQueryDetailsApiResponse response = asyncResourceTestClient.submitAsyncQuery(query);
      Assert.assertEquals(response.getState(), SqlAsyncQueryDetails.State.INITIALIZED);
      String asyncResultId = response.getAsyncResultId();
      ITRetryUtil.retryUntilTrue(
          () -> {
            SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
            return statusResponse.getState() == SqlAsyncQueryDetails.State.COMPLETE;
          },
          "Waiting for async task to be completed"
      );

      List<List<Object>> results = asyncResourceTestClient.getResults(asyncResultId);
      // Result should only contain one row
      Assert.assertEquals(results.size(), 1);
      Assert.assertTrue(resultFileExists(asyncResultId));

      // cancel query
      Assert.assertTrue(asyncResourceTestClient.cancel(asyncResultId));

      // status should be not found
      Assert.assertNull(asyncResourceTestClient.getStatus(asyncResultId));

      // result should be null
      Assert.assertNull(asyncResourceTestClient.getResults(asyncResultId));
      Assert.assertFalse(resultFileExists(asyncResultId));

      // issue cancel again
      Assert.assertFalse(asyncResourceTestClient.cancel(asyncResultId));
    }
  }

  private boolean resultFileExists(String asyncResultId) throws Exception
  {
    // result file check on the broker
    String lsFileResult = druidClusterAdminClient.runCommandInBrokerContainer(
        "bash", "-c", "ls /shared/storage/async-results/" + asyncResultId + " | wc -l"
    ).lhs;
    return "1".equals(StringUtils.chomp(lsFileResult));
  }

  @Test
  public void cancelRunningQuery() throws Exception
  {
    final SqlQuery query = new SqlQuery(
        "SELECT sleep(10)",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        null,
        null
    );
    SqlAsyncQueryDetailsApiResponse response = asyncResourceTestClient.submitAsyncQuery(query);
    Assert.assertEquals(response.getState(), SqlAsyncQueryDetails.State.INITIALIZED);
    String asyncResultId = response.getAsyncResultId();
    ITRetryUtil.retryUntilTrue(
        () -> {
          SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
          return statusResponse.getState() == SqlAsyncQueryDetails.State.RUNNING;
        },
        "Waiting for async task to be running"
    );

    // cancel query
    Assert.assertTrue(asyncResourceTestClient.cancel(asyncResultId));

    // status should be not found
    Assert.assertNull(asyncResourceTestClient.getStatus(asyncResultId));
    // result should be null
    Assert.assertNull(asyncResourceTestClient.getResults(asyncResultId));
    // issue cancel again
    Assert.assertFalse(asyncResourceTestClient.cancel(asyncResultId));
  }
}
