/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManagerImpl;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataStorageTableConfig;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails.State;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsAndMetadata;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManager;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManagerConfig;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import io.imply.druid.sql.async.result.SqlAsyncResults;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprMacroTable.ExprMacro;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expressions.SleepExprMacro;
import org.apache.druid.query.sql.SleepOperatorConversion;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class SqlAsyncResourceTest extends BaseCalciteQueryTest
{
  private static final int MAX_CONCURRENT_QUERIES = 2;
  private static final int MAX_QUERIES_TO_QUEUE = 2;
  private static final String BROKER_ID = "brokerId123";
  private static final long ASYNC_LAST_UPDATE_TIME_TRIGGER = 2;
  private static final long CLOCK_START_TIME = 100L;

  @Rule
  public final DerbyConnectorRule connectorRule = new DerbyConnectorRule();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final SqlAsyncMetadataStorageTableConfig tableConfig = new SqlAsyncMetadataStorageTableConfig(null, null);

  private SqlAsyncQueryPool queryPool;
  private SqlAsyncResource resource;
  private HttpServletRequest req;
  private SqlAsyncMetadataManagerImpl metadataManager;

  @Mock
  private Clock clock;

  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(
        ImmutableSet.of(),
        ImmutableSet.of(new SleepOperatorConversion())
    );
  }

  @Override
  public ExprMacroTable createMacroTable()
  {
    final List<ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(CalciteTests.INJECTOR.getInstance(clazz));
    }
    exprMacros.add(CalciteTests.INJECTOR.getInstance(LookupExprMacro.class));
    exprMacros.add(new SleepExprMacro());
    return new ExprMacroTable(exprMacros);
  }

  @Before
  public void setupTest() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    final File resultStorage = temporaryFolder.newFolder();
    metadataManager = new SqlAsyncMetadataManagerImpl(
        jsonMapper,
        MetadataStorageConnectorConfig::new,
        tableConfig,
        connectorRule.getConnector()
    );
    Mockito.when(clock.millis()).thenReturn(CLOCK_START_TIME);
    metadataManager.initialize();
    final Lifecycle lifecycle = new Lifecycle();
    final SqlAsyncResultManager resultManager = new LocalSqlAsyncResultManager(
        new LocalSqlAsyncResultManagerConfig()
        {
          @Override
          public String getDirectory()
          {
            return resultStorage.getAbsolutePath();
          }
        }
    );
    AsyncQueryConfig asyncQueryConfig = new TestableAsyncQueryConfig(
        MAX_CONCURRENT_QUERIES,
        MAX_QUERIES_TO_QUEUE,
        Duration.millis(ASYNC_LAST_UPDATE_TIME_TRIGGER)
    );
    SqlAsyncLifecycleManager sqlAsyncLifecycleManager = new SqlAsyncLifecycleManager(new SqlLifecycleManager());
    SqlAsyncModule.SqlAsyncQueryPoolProvider poolProvider = new SqlAsyncModule.SqlAsyncQueryPoolProvider(
        BROKER_ID,
        jsonMapper,
        asyncQueryConfig,
        metadataManager,
        resultManager,
        sqlAsyncLifecycleManager,
        lifecycle
    );
    queryPool = poolProvider.get();
    final SqlLifecycleFactory sqlLifecycleFactory = getSqlLifecycleFactory(
        new PlannerConfig(),
        createOperatorTable(),
        createMacroTable(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
    );
    resource = new SqlAsyncResource(
        BROKER_ID,
        queryPool,
        metadataManager,
        resultManager,
        sqlLifecycleFactory,
        sqlAsyncLifecycleManager,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        jsonMapper,
        asyncQueryConfig,
        clock,
        null, // Talaria only
        null  // Talaria only
    );
    req = mockAuthenticatedRequest("userID");
  }

  @After
  public void tearDownTest() throws IOException
  {
    queryPool.stop();
    connectorRule.getConnector().deleteAllRecords(tableConfig.getSqlAsyncQueriesTable());
  }

  @Test
  public void testSubmitQuery()
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, submitResponse.getEntity().getClass());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    Assert.assertEquals(State.INITIALIZED, response.getState());
  }

  @Test(timeout = 5000)
  public void testGetStatus()
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    response = waitUntilState(response.getAsyncResultId(), State.RUNNING);
    Assert.assertNull(response.getResultFormat());
    Assert.assertEquals(0, response.getResultLength());
    Assert.assertNull(response.getError());

    response = waitUntilState(response.getAsyncResultId(), State.COMPLETE);
    Assert.assertEquals(ResultFormat.OBJECTLINES, response.getResultFormat());
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 57 : 59, response.getResultLength());
    Assert.assertNull(response.getError());
  }

  @Test
  public void testGetStatusUnknownQuery()
  {
    Response statusResponse = resource.doGetStatus("unknownQuery", req);
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), statusResponse.getStatus());
    Assert.assertNull(statusResponse.getEntity());
  }

  @Test
  public void testGetResults() throws IOException
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select dim1, sum(m1) from foo group by 1",
            ResultFormat.CSV,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    waitUntilState(response.getAsyncResultId(), State.COMPLETE);

    Response resultsResponse = resource.doGetResults(response.getAsyncResultId(), req);
    SqlAsyncResults results = (SqlAsyncResults) resultsResponse.getEntity();
    Assert.assertEquals(55, results.getSize());
    byte[] buf = new byte[(int) results.getSize()];
    Assert.assertEquals(results.getSize(), results.getInputStream().read(buf));
    Assert.assertEquals(
        "dim1,EXPR$1\n"
        + ",1.0\n"
        + "1,4.0\n"
        + "10.1,2.0\n"
        + "2,3.0\n"
        + "abc,6.0\n"
        + "def,5.0\n"
        + "\n",
        StringUtils.fromUtf8(buf)
    );
  }

  @Test
  public void testGetResultsAndEnsureLastUpdateTimeIsAffected()
      throws IOException, InterruptedException
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select dim1, sum(m1) from foo group by 1",
            ResultFormat.CSV,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    waitUntilState(response.getAsyncResultId(), State.COMPLETE);

    Response resultsResponse = resource.doGetResults(response.getAsyncResultId(), req);
    Optional<SqlAsyncQueryDetailsAndMetadata> metadataResultsOpt = metadataManager.getQueryDetailsAndMetadata(response.getAsyncResultId());
    long beforeReadLastUpdateTime = metadataResultsOpt.get().getMetadata().getLastUpdatedTime();
    Thread.sleep(1);
    SqlAsyncResults results = (SqlAsyncResults) resultsResponse.getEntity();
    Assert.assertEquals(55, results.getSize());

    long currentTime = CLOCK_START_TIME;
    byte[] buf = new byte[(int) results.getSize()];
    for (int ii = 0; ii < results.getSize(); ii++) {
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TIME_TRIGGER);
      buf[ii] = (byte) results.getInputStream().read();
    }
    metadataResultsOpt = metadataManager.getQueryDetailsAndMetadata(response.getAsyncResultId());
    long postReadLastUpdateTime = metadataResultsOpt.get().getMetadata().getLastUpdatedTime();
    Assert.assertTrue(postReadLastUpdateTime > beforeReadLastUpdateTime);
    Assert.assertEquals(
        "dim1,EXPR$1\n"
        + ",1.0\n"
        + "1,4.0\n"
        + "10.1,2.0\n"
        + "2,3.0\n"
        + "abc,6.0\n"
        + "def,5.0\n"
        + "\n",
        StringUtils.fromUtf8(buf)
    );
  }

  @Test
  public void testGetResultsAndEnsureLastUpdateTimeIsNotAffectedIfTimeStandsStill()
      throws IOException, InterruptedException
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select dim1, sum(m1) from foo group by 1",
            ResultFormat.CSV,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    waitUntilState(response.getAsyncResultId(), State.COMPLETE);

    Response resultsResponse = resource.doGetResults(response.getAsyncResultId(), req);
    Optional<SqlAsyncQueryDetailsAndMetadata> metadataResultsOpt = metadataManager.getQueryDetailsAndMetadata(response.getAsyncResultId());
    long beforeReadLastUpdateTime = metadataResultsOpt.get().getMetadata().getLastUpdatedTime();
    Thread.sleep(1);
    SqlAsyncResults results = (SqlAsyncResults) resultsResponse.getEntity();
    Assert.assertEquals(55, results.getSize());

    byte[] buf = new byte[(int) results.getSize()];
    for (int ii = 0; ii < results.getSize(); ii++) {
      Mockito.when(clock.millis()).thenReturn(CLOCK_START_TIME);
      buf[ii] = (byte) results.getInputStream().read();
    }
    metadataResultsOpt = metadataManager.getQueryDetailsAndMetadata(response.getAsyncResultId());
    long postReadLastUpdateTime = metadataResultsOpt.get().getMetadata().getLastUpdatedTime();
    Assert.assertEquals(beforeReadLastUpdateTime, postReadLastUpdateTime);
    Assert.assertEquals(
        "dim1,EXPR$1\n"
        + ",1.0\n"
        + "1,4.0\n"
        + "10.1,2.0\n"
        + "2,3.0\n"
        + "abc,6.0\n"
        + "def,5.0\n"
        + "\n",
        StringUtils.fromUtf8(buf)
    );
  }

  @Test
  public void testGetResultsUnknownQuery()
  {
    Response statusResponse = resource.doGetResults("unknownQuery", req);
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), statusResponse.getStatus());
    Assert.assertNull(statusResponse.getEntity());
  }

  @Test(timeout = 5000)
  public void testConcurrentAsyncQueryLimit()
  {
    List<String> queryIds = new ArrayList<>();
    // Submit MAX_CONCURRENT_QUERIES + 1 number of queries
    for (int i = 0; i < MAX_CONCURRENT_QUERIES + 1; i++) {
      Response submitResponse = resource.doPost(
          new SqlQuery(
              "select sleep(2), 10",
              ResultFormat.OBJECTLINES,
              true,
              false,
              false,
              null,
              null
          ),
          req
      );
      Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
      Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, submitResponse.getEntity().getClass());
      SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
      queryIds.add(response.getAsyncResultId());
    }

    // Wait for queries submitted to start running
    waitUntilState(queryIds.get(MAX_CONCURRENT_QUERIES - 1), State.RUNNING);

    // Now the first MAX_CONCURRENT_QUERIES queries should be RUNNING
    for (int i = 0; i < MAX_CONCURRENT_QUERIES; i++) {
      Response statusResponse = resource.doGetStatus(queryIds.get(i), req);
      Assert.assertEquals(Status.OK.getStatusCode(), statusResponse.getStatus());
      Assert.assertEquals(State.RUNNING, ((SqlAsyncQueryDetailsApiResponse) statusResponse.getEntity()).getState());
    }
    SqlAsyncQueryPool.BestEffortStatsSnapshot sqlAsyncQueryPoolStats = queryPool.getBestEffortStatsSnapshot();
    Assert.assertEquals(MAX_CONCURRENT_QUERIES, sqlAsyncQueryPoolStats.getQueryRunningCount());

    // The last query that was over MAX_CONCURRENT_QUERIES limit should still be in INITIALIZED state
    Response statusResponse = resource.doGetStatus(queryIds.get(MAX_CONCURRENT_QUERIES), req);
    Assert.assertEquals(Status.OK.getStatusCode(), statusResponse.getStatus());
    Assert.assertEquals(State.INITIALIZED, ((SqlAsyncQueryDetailsApiResponse) statusResponse.getEntity()).getState());
    Assert.assertEquals(1, sqlAsyncQueryPoolStats.getQueryQueuedCount());
  }

  @Test(timeout = 5000)
  public void testQueueLimit()
  {
    // Submit MAX_CONCURRENT_QUERIES + MAX_QUERIES_TO_QUEUE number of queries
    for (int i = 0; i < MAX_CONCURRENT_QUERIES + MAX_QUERIES_TO_QUEUE; i++) {
      Response submitResponse = resource.doPost(
          new SqlQuery(
              "select sleep(2), 10",
              ResultFormat.OBJECTLINES,
              true,
              false,
              false,
              null,
              null
          ),
          req
      );
      Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
      Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, submitResponse.getEntity().getClass());
      SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
      Assert.assertEquals(State.INITIALIZED, response.getState());
    }
    SqlAsyncQueryPool.BestEffortStatsSnapshot sqlAsyncQueryPoolStats = queryPool.getBestEffortStatsSnapshot();
    Assert.assertEquals(MAX_CONCURRENT_QUERIES, sqlAsyncQueryPoolStats.getQueryRunningCount());
    Assert.assertEquals(MAX_QUERIES_TO_QUEUE, sqlAsyncQueryPoolStats.getQueryQueuedCount());
    // Now submit one more so that we will exceed the queue limit
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, submitResponse.getStatus());
  }

  @Test(timeout = 5000)
  public void testQueryPoolShutdown() throws IOException
  {
    List<String> queryIds = new ArrayList<>();

    // Submit MAX_SIMULTANEOUS_QUERY_LIMIT + MAX_QUERY_QUEUE_SIZE_LIMIT number of queries
    for (int i = 0; i < MAX_CONCURRENT_QUERIES + MAX_QUERIES_TO_QUEUE; i++) {
      Response submitResponse = resource.doPost(
          new SqlQuery(
              "select sleep(2), 10",
              ResultFormat.OBJECTLINES,
              true,
              false,
              false,
              null,
              null
          ),
          req
      );
      Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
      Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, submitResponse.getEntity().getClass());
      SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
      Assert.assertEquals(State.INITIALIZED, response.getState());
      queryIds.add(response.getAsyncResultId());
    }

    queryPool.stop();

    for (String queryId : queryIds) {
      Response statusResponse = resource.doGetStatus(queryId, req);
      Assert.assertEquals(Status.OK.getStatusCode(), statusResponse.getStatus());
      Assert.assertEquals(State.FAILED, ((SqlAsyncQueryDetailsApiResponse) statusResponse.getEntity()).getState());
    }
  }

  @Test(timeout = 5000)
  public void testDeleteRunningQuery()
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    response = waitUntilState(response.getAsyncResultId(), State.RUNNING);
    Response cancelResponse = resource.deleteQuery(response.getAsyncResultId(), req);
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), cancelResponse.getStatus());
    Assert.assertEquals(
        Status.NOT_FOUND.getStatusCode(),
        resource.doGetStatus(response.getAsyncResultId(), req).getStatus()
    );
    Assert.assertEquals(
        Status.NOT_FOUND.getStatusCode(),
        resource.deleteQuery(response.getAsyncResultId(), req).getStatus()
    );
    Assert.assertNull(response.getError());
  }

  @Test(timeout = 5000)
  public void testDeleteCompletedQuery()
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select dim1, sum(m1) from foo group by 1",
            ResultFormat.CSV,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    waitUntilState(response.getAsyncResultId(), State.COMPLETE);

    Response resultsResponse = resource.doGetResults(response.getAsyncResultId(), req);
    SqlAsyncResults results = (SqlAsyncResults) resultsResponse.getEntity();
    Assert.assertEquals(55, results.getSize());

    Response cancelResponse = resource.deleteQuery(response.getAsyncResultId(), req);
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), cancelResponse.getStatus());
    Assert.assertEquals(
        Status.NOT_FOUND.getStatusCode(),
        resource.doGetStatus(response.getAsyncResultId(), req).getStatus()
    );
    // delete twice
    Assert.assertEquals(
        Status.NOT_FOUND.getStatusCode(),
        resource.deleteQuery(response.getAsyncResultId(), req).getStatus()
    );

    Assert.assertEquals(
        Status.NOT_FOUND.getStatusCode(),
        resource.doGetResults(response.getAsyncResultId(), req).getStatus()
    );
    Assert.assertNull(response.getError());
  }

  @Test(timeout = 5000)
  public void testDeleteUnkownQuery()
  {
    Response statusResponse = resource.deleteQuery("unknownQuery", req);
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), statusResponse.getStatus());
    Assert.assertNull(statusResponse.getEntity());
  }

  @Test(timeout = 5000)
  public void testForbidden()
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select 1,2",
            ResultFormat.CSV,
            true,
            false,
            false,
            null,
            null
        ),
        req
    );

    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
    SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
    waitUntilState(response.getAsyncResultId(), State.COMPLETE);

    HttpServletRequest secondUser = makeRandomUserRequest();
    try {
      resource.doGetStatus(response.getAsyncResultId(), secondUser);
      Assert.fail();
    }
    catch (ForbiddenException e) {
      // expected
    }
    try {
      resource.doGetResults(response.getAsyncResultId(), secondUser);
      Assert.fail();
    }
    catch (ForbiddenException e) {
      // expected
    }
    try {
      resource.deleteQuery(response.getAsyncResultId(), secondUser);
      Assert.fail();
    }
    catch (ForbiddenException e) {
      // expected
    }
  }

  private SqlAsyncQueryDetailsApiResponse waitUntilState(String asyncResultId, State state)
  {
    SqlAsyncQueryDetailsApiResponse response = null;

    while (response == null || response.getState() != state) {
      Response statusResponse = resource.doGetStatus(asyncResultId, req);
      Assert.assertEquals(Status.OK.getStatusCode(), statusResponse.getStatus());
      response = (SqlAsyncQueryDetailsApiResponse) statusResponse.getEntity();
    }
    return response;
  }

  private static HttpServletRequest mockAuthenticatedRequest(String user)
  {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult(user, "testAuthorizor", "testAuthenticator", null));
    return request;
  }

  private static HttpServletRequest makeRandomUserRequest()
  {
    return mockAuthenticatedRequest(UUID.randomUUID().toString());
  }

  // This class is used to get around the minimums used in the actual AsyncQueryConfig class
  private static class TestableAsyncQueryConfig extends AsyncQueryConfig
  {
    private final Duration readRefreshTime;

    public TestableAsyncQueryConfig(
        @Nullable Integer maxConcurrentQueries,
        @Nullable Integer maxQueriesToQueue,
        @Nullable Duration readRefreshTime
    )
    {
      super(maxConcurrentQueries, maxQueriesToQueue, Duration.standardSeconds(1L));
      this.readRefreshTime = readRefreshTime;
    }

    @Override
    public Duration getReadRefreshTime()
    {
      return readRefreshTime;
    }
  }
}
