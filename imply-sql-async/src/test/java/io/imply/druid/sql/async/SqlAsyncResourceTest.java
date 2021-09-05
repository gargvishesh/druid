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
import io.imply.druid.sql.async.SqlAsyncQueryDetails.State;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprMacroTable.ExprMacro;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expressions.SleepExprMacro;
import org.apache.druid.query.sql.SleepOperatorConversion;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class SqlAsyncResourceTest extends BaseCalciteQueryTest
{
  private static final int MAX_CONCURRENT_QUERIES = 2;
  private static final int MAX_ASYNC_QUERIES = 6;
  private static final int MAX_QUERIES_TO_QUEUE = 2;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private SqlAsyncQueryPool queryPool;
  private SqlAsyncResource resource;
  private HttpServletRequest req;

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
    final File resultStorage = temporaryFolder.newFolder();
    final SqlAsyncMetadataManager metadataManager = new InMemorySqlAsyncMetadataManager();
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
    AsyncQueryLimitsConfig asyncQueryLimitsConfig = new AsyncQueryLimitsConfig(
        MAX_CONCURRENT_QUERIES,
        MAX_ASYNC_QUERIES,
        MAX_QUERIES_TO_QUEUE
    );
    SqlAsyncModule.SqlAsyncQueryPoolProvider poolProvider = new SqlAsyncModule.SqlAsyncQueryPoolProvider(
        metadataManager,
        resultManager,
        jsonMapper,
        asyncQueryLimitsConfig,
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
        "brokerId",
        queryPool,
        metadataManager,
        resultManager,
        sqlLifecycleFactory,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        jsonMapper
    );
    req = mockAuthenticatedRequest();
  }

  @After
  public void tearDownTest()
  {
    queryPool.shutdownNow();
  }

  @Test
  public void testSubmitQuery() throws IOException
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
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
  public void testGetStatus() throws IOException
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
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
  public void testGetStatusUnknownQuery() throws IOException
  {
    Response statusResponse = resource.doGetStatus("unknownQuery", req);
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), statusResponse.getStatus());
  }

  @Test
  public void testGetResults() throws IOException
  {
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select dim1, sum(m1) from foo group by 1",
            ResultFormat.CSV,
            true,
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
  public void testGetResultsUnknownQuery() throws IOException
  {
    Response statusResponse = resource.doGetResults("unknownQuery", req);
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), statusResponse.getStatus());
  }

  @Test(timeout = 5000)
  public void testConcurrentAsyncQueryLimit() throws Exception
  {
    List<String> queryIds = new ArrayList<>();
    // Submit MAX_CONCURRENT_QUERIES + 1 number of queries
    for (int i = 0; i < MAX_CONCURRENT_QUERIES + 1; i++) {
      Response submitResponse = resource.doPost(
          new SqlQuery(
              "select sleep(2), 10",
              ResultFormat.OBJECTLINES,
              true,
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

    // The last query that was over MAX_CONCURRENT_QUERIES limit should still be in INITIALIZED state
    Response statusResponse = resource.doGetStatus(queryIds.get(MAX_CONCURRENT_QUERIES), req);
    Assert.assertEquals(Status.OK.getStatusCode(), statusResponse.getStatus());
    Assert.assertEquals(State.INITIALIZED, ((SqlAsyncQueryDetailsApiResponse) statusResponse.getEntity()).getState());
  }

  @Test(timeout = 5000)
  public void testQueueLimit() throws Exception
  {
    // Submit MAX_CONCURRENT_QUERIES + MAX_QUERIES_TO_QUEUE number of queries
    for (int i = 0; i < MAX_CONCURRENT_QUERIES + MAX_QUERIES_TO_QUEUE; i++) {
      Response submitResponse = resource.doPost(
          new SqlQuery(
              "select sleep(2), 10",
              ResultFormat.OBJECTLINES,
              true,
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
    // Now submit one more so that we will exceed the queue limit
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, submitResponse.getStatus());
  }

  @Test(timeout = 5000)
  public void testRetentionNumberOfQueriesLimit() throws Exception
  {
    // Submit MAX_ASYNC_QUERIES number of queries
    for (int i = 0; i < MAX_ASYNC_QUERIES; i++) {
      Response submitResponse = resource.doPost(
          new SqlQuery(
              "select 10",
              ResultFormat.OBJECTLINES,
              true,
              null,
              null
          ),
          req
      );
      Assert.assertEquals(Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
      Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, submitResponse.getEntity().getClass());
      SqlAsyncQueryDetailsApiResponse response = (SqlAsyncQueryDetailsApiResponse) submitResponse.getEntity();
      Assert.assertEquals(State.INITIALIZED, response.getState());
      response = waitUntilState(response.getAsyncResultId(), State.COMPLETE);
      Assert.assertNull(response.getError());
    }
    // Now submit one more so that we will exceed the retention limit
    Response submitResponse = resource.doPost(
        new SqlQuery(
            "select sleep(2), 10",
            ResultFormat.OBJECTLINES,
            true,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, submitResponse.getStatus());
  }

  private SqlAsyncQueryDetailsApiResponse waitUntilState(String asyncResultId, State state) throws IOException
  {
    SqlAsyncQueryDetailsApiResponse response = null;

    while (response == null || response.getState() != state) {
      Response statusResponse = resource.doGetStatus(asyncResultId, req);
      Assert.assertEquals(Status.OK.getStatusCode(), statusResponse.getStatus());
      response = (SqlAsyncQueryDetailsApiResponse) statusResponse.getEntity();
    }
    return response;
  }

  private static HttpServletRequest mockAuthenticatedRequest()
  {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult("userId", "testAuthorizor", "testAuthenticator", null));
    return request;
  }
}
