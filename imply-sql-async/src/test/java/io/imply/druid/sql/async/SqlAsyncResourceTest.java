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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
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
import java.util.concurrent.ExecutorService;

@RunWith(MockitoJUnitRunner.class)
public class SqlAsyncResourceTest extends BaseCalciteQueryTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private ExecutorService exec;
  private SqlAsyncResource resource;
  private HttpServletRequest req;

  @Before
  public void setupTest() throws IOException
  {
    final File resultStorage = temporaryFolder.newFolder();
    final SqlAsyncMetadataManager metadataManager = new InMemorySqlAsyncMetadataManager();
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
    exec = Execs.multiThreaded(2, "sql-async-resource-test-%d");
    final SqlAsyncQueryPool queryPool = new SqlAsyncQueryPool(
        exec,
        metadataManager,
        resultManager,
        jsonMapper
    );
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
    exec.shutdownNow();
  }

  @Test
  public void testSubmitQuery() throws IOException
  {
    Response response = resource.doPost(
        new SqlQuery(
            "select count(*) from foo",
            ResultFormat.CSV,
            true,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());
    Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, response.getEntity().getClass());
  }

  @Test
  public void testGetStatus() throws IOException
  {
    Response response = resource.doPost(
        new SqlQuery(
            "select sleep(5)",
            ResultFormat.CSV,
            true,
            null,
            null
        ),
        req
    );
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());
    Assert.assertSame(SqlAsyncQueryDetailsApiResponse.class, response.getEntity().getClass());
  }

  private static HttpServletRequest mockAuthenticatedRequest()
  {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult("userId", "testAuthorizor", "testAuthenticator", null));
    return request;
  }
}
