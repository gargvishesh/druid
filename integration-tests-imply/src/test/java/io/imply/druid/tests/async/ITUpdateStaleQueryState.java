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
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails.State;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import io.imply.druid.tests.ImplyTestNGGroup;
import io.imply.druid.tests.client.AsyncResourceTestClient;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DruidClusterAdminClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = ImplyTestNGGroup.ASYNC_DOWNLOAD)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITUpdateStaleQueryState
{
  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  @Inject
  private AsyncResourceTestClient asyncResourceTestClient;

  @Test
  public void testUpdateStaleQueryState() throws Exception
  {
    final SqlQuery query = new SqlQuery(
        "SELECT sleep(6000), 10",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        null,
        null
    );
    final SqlAsyncQueryDetailsApiResponse response = asyncResourceTestClient.submitAsyncQuery(query);
    Assert.assertEquals(response.getState(), SqlAsyncQueryDetails.State.INITIALIZED);
    final String asyncResultId = response.getAsyncResultId();

    druidClusterAdminClient.killAndRestartBrokerContainer();

    // Wait until broker is ready again
    druidClusterAdminClient.waitUntilBrokerReady();

    ITRetryUtil.retryUntilTrue(
        () -> {
          SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
          return statusResponse.getState().isFinal();
        },
        StringUtils.format("Wating for query to finish [%s]", asyncResultId)
    );

    SqlAsyncQueryDetailsApiResponse statusResponse = asyncResourceTestClient.getStatus(asyncResultId);
    Assert.assertEquals(statusResponse.getState(), State.UNDETERMINED);

    Assert.assertTrue(asyncResourceTestClient.cancel(asyncResultId));
  }
}
