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
import io.imply.druid.sql.async.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.SqlAsyncQueryDetailsApiResponse;
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
import java.util.concurrent.TimeUnit;

@Test(groups = ImplyTestNGGroup.ASYNC_DOWNLOAD)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAsyncCleanupCoordinatorDuty extends AbstractIndexerTest
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
  public void testKillAsyncQueryMetadataAfterPassedRetainTime() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      loadData(INDEX_TASK, fullDatasourceName);
      final SqlQuery query = new SqlQuery(
          "SELECT count(*) FROM \"" + fullDatasourceName + "\"",
          ResultFormat.ARRAY,
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

      List<List<Object>> results = asyncResourceTestClient.getResult(asyncResultId);
      // Result should only contain one row
      Assert.assertEquals(results.size(), 1);
      // The row should only have one column
      Assert.assertEquals(results.get(0).size(), 1);
      // The count(*) value should equal 10
      Assert.assertEquals(results.get(0).get(0), 10);

      // Cleanup should finish in no more than 35 seconds as retain time is set to 30 seconds and duty cycle is run
      // every 5 seconds.
      ITRetryUtil.retryUntil(
          () -> {
            SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
            return statusResponse == null;
          },
          true,
          TimeUnit.SECONDS.toMillis(5),
          8,
          "Wating for async cleanup coordinator duty to complete"
      );

      Assert.assertNull(asyncResourceTestClient.getResult(asyncResultId));
    }
  }

  // Note: This test depends on the Druid cluster using local asyncstorage with asyncstorage directory at path /shared/storage/async-results
  @Test
  public void testKillAsyncQueryResultWithoutMetadata() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      loadData(INDEX_TASK, fullDatasourceName);
      final SqlQuery query = new SqlQuery(
          "SELECT count(*) FROM \"" + fullDatasourceName + "\"",
          ResultFormat.ARRAY,
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

      // Create file without metadata and check that the created file exist
      String createFileResult = druidClusterAdminClient.runCommandInBrokerContainer(
          "bash", "-c", "touch /shared/storage/async-results/filewithnometdata; test -f /shared/storage/async-results/filewithnometdata; echo $?"
      ).lhs;
      Assert.assertEquals(
          StringUtils.chomp(createFileResult),
          "0"
      );

      // Wait for duty to remove the created file
      ITRetryUtil.retryUntilTrue(
          () -> {
            String stdout = druidClusterAdminClient.runCommandInBrokerContainer("bash", "-c", "test -f /shared/storage/async-results/filewithnometdata; echo $?").lhs;
            String fileCheckResult = StringUtils.chomp(stdout);
            return "1".equals(fileCheckResult);
          },
          "Waiting for cleanup to be completed"
      );

      // Verify that async result with metadata was not deleted by the duty
      List<List<Object>> results = asyncResourceTestClient.getResult(asyncResultId);
      // Result should only contain one row
      Assert.assertEquals(results.size(), 1);
      // The row should only have one column
      Assert.assertEquals(results.get(0).size(), 1);
      // The count(*) value should equal 10
      Assert.assertEquals(results.get(0).get(0), 10);
    }
  }
}
