/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SqlAsyncMetadataManagerImplConcurrencyTest
{
  private static final int THREAD_POOL_SIZE = 4;
  @Rule
  public final DerbyConnectorRule connectorRule = new DerbyConnectorRule();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private ExecutorService exec;
  private SqlAsyncMetadataManagerImpl metadataManager;

  @Before
  public void setup()
  {
    exec = Execs.multiThreaded(THREAD_POOL_SIZE, "SqlAsyncMetadataManagerImplConcurrencyTest-%s");
    metadataManager = new SqlAsyncMetadataManagerImpl(
        jsonMapper,
        MetadataStorageConnectorConfig::new,
        new SqlAsyncMetadataStorageTableConfig(null, null),
        connectorRule.getConnector()
    );
    metadataManager.initialize();
  }

  @After
  public void tearDown()
  {
    exec.shutdownNow();
  }

  @Test
  public void testOneThreadUpdatesStateToNonFinalBeforeOtherThreadUpdates() throws Exception
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);
    CountDownLatch latch = new CountDownLatch(1);
    SqlAsyncQueryDetails expected = queryDetails.toComplete(10L);

    Future<Boolean> f2 = exec.submit(() -> {
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      return metadataManager.updateQueryDetails(expected);
    });
    Future<Boolean> f1 = exec.submit(() -> {
      boolean result = metadataManager.updateQueryDetails(queryDetails.toRunning());
      latch.countDown();
      return result;
    });

    Assert.assertTrue(f1.get(5, TimeUnit.SECONDS));
    Assert.assertTrue(f2.get(5, TimeUnit.SECONDS));
    SqlAsyncMetadataManagerImplTest.assertQueryState(metadataManager, "id", expected);
  }

  @Test
  public void testOneThreadUpdatesStateToFinalBeforeOtherThreadUpdates() throws Exception
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);
    CountDownLatch latch = new CountDownLatch(1);
    SqlAsyncQueryDetails expected = queryDetails.toUndetermined();

    Future<Boolean> f2 = exec.submit(() -> {
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      return metadataManager.updateQueryDetails(queryDetails.toComplete(10L));
    });
    Future<Boolean> f1 = exec.submit(() -> {
      boolean result = metadataManager.updateQueryDetails(expected);
      latch.countDown();
      return result;
    });

    Assert.assertTrue(f1.get(5, TimeUnit.SECONDS));
    Assert.assertFalse(f2.get(5, TimeUnit.SECONDS));
    SqlAsyncMetadataManagerImplTest.assertQueryState(metadataManager, "id", expected);
  }

  @Test
  public void testManyThreadsUpdatesToNonFinalAndOneThreadUpdatesToFinal() throws Exception
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);

    List<Future<Boolean>> futures = new ArrayList<>();
    futures.add(exec.submit(() -> metadataManager.updateQueryDetails(queryDetails.toError(null))));
    for (int i = 0; i < THREAD_POOL_SIZE - 1; i++) {
      futures.add(exec.submit(() -> metadataManager.updateQueryDetails(queryDetails.toRunning())));
    }
    boolean resultForUpdateToFinalState;
    resultForUpdateToFinalState = futures.get(0).get(5, TimeUnit.SECONDS);
    for (Future<Boolean> future : futures) {
      future.get(5, TimeUnit.SECONDS);
    }

    // We only care about the result of updating to a final state, which should be success.
    Assert.assertTrue(resultForUpdateToFinalState);
  }
}
