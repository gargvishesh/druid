/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.grpc;

import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.imply.druid.grpc.server.GrpcQueryConfig;
import io.imply.druid.grpc.server.QueryDriver;
import io.imply.druid.grpc.server.QueryServer;
import org.apache.druid.grpc.proto.QueryOuterClass;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.validator.CredentialsValidator;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Simple test that runs the gRPC server, on top of a test SQL stack.
 * Uses a simple client to send a query to the server. This is a basic
 * sanity check of the gRPC stack. Uses allow-all security, which
 * does a sanity check of the auth chain.
 */
public class BasicAuthTest
{
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static QueryFrameworkFixture frameworkFixture;
  private static QueryServer server;

  @BeforeClass
  public static void setup() throws IOException
  {
    frameworkFixture = new QueryFrameworkFixture(temporaryFolder.newFolder());
    QueryDriver driver = new QueryDriver(
        frameworkFixture.jsonMapper(),
        frameworkFixture.statementFactory(),
        frameworkFixture.getQueryLifecycleFactory()
    );
    CredentialsValidator validator = new CredentialsValidator()
    {
      @Override
      public AuthenticationResult validateCredentials(String authenticatorName, String authorizerName,
          String username, char[] password)
      {
        if (CalciteTests.TEST_SUPERUSER_NAME.equals(username)) {
          if (!"secret".equals(new String(password))) {
            return null;
          }
          return CalciteTests.SUPER_USER_AUTH_RESULT;
        }
        if ("regular".equals(username)) {
          if (!"pwd".equals(new String(password))) {
            return null;
          }
          return CalciteTests.REGULAR_USER_AUTH_RESULT;
        }
        return null;
      }
    };
    BasicHTTPAuthenticator basicAuth = new BasicHTTPAuthenticator(
        null,
        "test",
        "test",
        new DefaultPasswordProvider("druid"),
        new DefaultPasswordProvider("druid"),
        null,
        null,
        null,
        false,
        validator
    );
    AuthenticatorMapper authMapper = new AuthenticatorMapper(
        ImmutableMap.of(
            "test",
            basicAuth
        )
    );
    GrpcQueryConfig config = new GrpcQueryConfig(50051);
    server = new QueryServer(config, driver, authMapper);
    try {
      server.start();
    }
    catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws InterruptedException
  {
    if (server != null) {
      server.stop();
      server.blockUntilShutdown();
    }
  }

  @Test
  public void testMissingAuth()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM foo")
        .setResultFormat(QueryResultFormat.CSV)
        .setQueryType(QueryOuterClass.QueryType.SQL)
        .build();

    try (TestClient client = new TestClient(TestClient.DEFAULT_HOST)) {
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.getQueryClient().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }

  @Test
  public void testInvalidUser()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM foo")
        .setResultFormat(QueryResultFormat.CSV)
        .setQueryType(QueryOuterClass.QueryType.SQL)
        .build();

    try (TestClient client = new TestClient(TestClient.DEFAULT_HOST, "invalid", "pwd")) {
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.getQueryClient().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }

  @Test
  public void testInvalidPassword()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM foo")
        .setResultFormat(QueryResultFormat.CSV)
        .setQueryType(QueryOuterClass.QueryType.SQL)
        .build();

    try (TestClient client = new TestClient(TestClient.DEFAULT_HOST, CalciteTests.TEST_SUPERUSER_NAME, "invalid")) {
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.getQueryClient().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }

  @Test
  public void testValidUser()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM foo")
        .setResultFormat(QueryResultFormat.CSV)
        .setQueryType(QueryOuterClass.QueryType.SQL)
        .build();

    try (TestClient client = new TestClient(TestClient.DEFAULT_HOST, CalciteTests.TEST_SUPERUSER_NAME, "secret")) {
      QueryResponse response = client.getQueryClient().submitQuery(request);
      assertEquals(QueryStatus.OK, response.getStatus());
    }
    try (TestClient client = new TestClient(TestClient.DEFAULT_HOST, "regular", "pwd")) {
      QueryResponse response = client.getQueryClient().submitQuery(request);
      assertEquals(QueryStatus.OK, response.getStatus());
    }
  }

  @Test
  public void testUnauthorized()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM forbiddenDatasource")
        .setResultFormat(QueryResultFormat.CSV)
        .setQueryType(QueryOuterClass.QueryType.SQL)
        .build();

    try (TestClient client = new TestClient(TestClient.DEFAULT_HOST, "regular", "pwd")) {
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.getQueryClient().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }
}
