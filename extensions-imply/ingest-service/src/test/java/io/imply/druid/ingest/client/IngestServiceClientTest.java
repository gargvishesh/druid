/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.ingest.IngestService;
import io.imply.druid.ingest.server.IngestJobsResponse;
import io.imply.druid.ingest.server.SchemasResponse;
import io.imply.druid.ingest.server.TablesResponse;
import io.imply.druid.sql.calcite.schema.IngestServiceSchemaTest;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class IngestServiceClientTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final NodeRole INGEST_ROLE = new NodeRole(IngestService.SERVICE_NAME);
  private static final DiscoveryDruidNode INGEST_NODE = new DiscoveryDruidNode(
      new DruidNode(
          IngestService.SERVICE_NAME,
          "localhost",
          false,
          IngestService.PORT,
          IngestService.PORT,
          IngestService.TLS_PORT,
          true,
          true
      ),
      INGEST_ROLE,
      ImmutableMap.of()
  );

  HttpClient httpClient;
  DruidNodeDiscoveryProvider discoParty;
  DruidNodeDiscovery discoBall;
  IngestServiceClient client;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    httpClient = EasyMock.createMock(HttpClient.class);
    discoParty = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    discoBall = EasyMock.createMock(DruidNodeDiscovery.class);
    client = new IngestServiceClient(httpClient, discoParty, MAPPER);
  }

  @After
  public void teardown()
  {
    EasyMock.verify(httpClient, discoParty, discoBall);
  }

  @Test
  public void testGetTables() throws IOException, ExecutionException, InterruptedException
  {
    final TablesResponse expectedResponse = new TablesResponse(
        ImmutableList.of(IngestServiceSchemaTest.TEST_TABLE, IngestServiceSchemaTest.OTHER_TEST_TABLE)
    );
    testDiscoAndGet(expectedResponse, IngestServiceClient.INGEST_TABLES_PATH, () -> client.getTables());
  }

  @Test
  public void testGetSchemas() throws ExecutionException, InterruptedException, IOException
  {
    final SchemasResponse expectedResponse = new SchemasResponse(
        ImmutableList.of(IngestServiceSchemaTest.TEST_SCHEMA, IngestServiceSchemaTest.OTHER_TEST_SCHEMA)
    );
    testDiscoAndGet(expectedResponse, IngestServiceClient.INGEST_SCHEMAS_PATH, () -> client.getSchemas());
  }

  @Test
  public void testGetJobs() throws InterruptedException, ExecutionException, IOException
  {
    final IngestJobsResponse expectedResponse = new IngestJobsResponse(
        ImmutableList.of(IngestServiceSchemaTest.TEST_JOB, IngestServiceSchemaTest.OTHER_TEST_JOB)
    );
    testDiscoAndGet(expectedResponse, IngestServiceClient.INGEST_JOBS_PATH, () -> client.getJobs());
  }

  @Test
  public void testGetTablesBadResponse() throws IOException, ExecutionException, InterruptedException
  {
    testDiscoAndGetBadResponse(IngestServiceClient.INGEST_TABLES_PATH, () -> client.getTables());
  }

  @Test
  public void testGetSchemasBadResponse() throws ExecutionException, InterruptedException, IOException
  {
    testDiscoAndGetBadResponse(IngestServiceClient.INGEST_SCHEMAS_PATH, () -> client.getSchemas());
  }

  @Test
  public void testGetJobsBadResponse() throws InterruptedException, ExecutionException, IOException
  {
    testDiscoAndGetBadResponse(IngestServiceClient.INGEST_JOBS_PATH, () -> client.getJobs());
  }

  @Test
  public void testGetTablesNoDisco() throws IOException
  {
    expectNoDisco();
    replayAll();
    client.getTables();
  }

  @Test
  public void testGetSchemasNoDisco() throws IOException
  {
    expectNoDisco();
    replayAll();
    client.getSchemas();
  }

  @Test
  public void testGetJobsNoDisco() throws IOException
  {
    expectNoDisco();
    replayAll();
    client.getJobs();
  }

  private <T> void testDiscoAndGet(T expectedResponse, String expectedPath, IngestClientGetFunctionRunner<T> runner)
      throws IOException, ExecutionException, InterruptedException
  {
    expectDisco();

    InputStreamFullResponseHolder responseHolder = EasyMock.createMock(InputStreamFullResponseHolder.class);
    ListenableFuture<Object> future = EasyMock.createMock(ListenableFuture.class);
    EasyMock.expect(future.get()).andReturn(responseHolder).once();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).once();
    EasyMock.expect(responseHolder.getContent())
            .andReturn(new ByteArrayInputStream(MAPPER.writeValueAsBytes(expectedResponse)))
            .once();
    Request expectedRequest = IngestServiceClient.getRequestForNode(INGEST_NODE, expectedPath);
    Capture<Request> capture = EasyMock.newCapture();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capture),
            EasyMock.anyObject()
        )
    ).andReturn(future).once();
    replayAll();
    EasyMock.replay(responseHolder, future);

    final T actualResponse = runner.run();

    Assert.assertEquals(expectedRequest.getUrl(), capture.getValue().getUrl());
    Assert.assertEquals(expectedRequest.getMethod(), capture.getValue().getMethod());
    Assert.assertEquals(expectedResponse, actualResponse);

    EasyMock.verify(responseHolder, future);
  }

  private <T> void testDiscoAndGetBadResponse(String expectedPath, IngestClientGetFunctionRunner<T> runner)
      throws IOException, ExecutionException, InterruptedException
  {
    expectedException.expect(RE.class);
    expectedException.expectMessage("Failed to talk to node at");

    expectDisco();
    InputStreamFullResponseHolder responseHolder = EasyMock.createMock(InputStreamFullResponseHolder.class);
    ListenableFuture<Object> future = EasyMock.createMock(ListenableFuture.class);
    EasyMock.expect(future.get()).andReturn(responseHolder).once();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR).times(3);
    Request expectedRequest = IngestServiceClient.getRequestForNode(INGEST_NODE, expectedPath);
    Capture<Request> capture = EasyMock.newCapture();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capture),
            EasyMock.anyObject()
        )
    ).andReturn(future).once();
    replayAll();
    EasyMock.replay(responseHolder, future);
    try {
      runner.run();
    }
    catch (RuntimeException ex) {
      Assert.assertEquals(expectedRequest.getUrl(), capture.getValue().getUrl());
      Assert.assertEquals(expectedRequest.getMethod(), capture.getValue().getMethod());
      EasyMock.verify(responseHolder, future);
      throw ex;
    }
  }

  private void expectDisco()
  {
    EasyMock.expect(discoParty.getForNodeRole(INGEST_ROLE)).andReturn(discoBall).anyTimes();
    EasyMock.expect(discoBall.getAllNodes()).andReturn(ImmutableList.of(INGEST_NODE)).anyTimes();
  }

  private void expectNoDisco()
  {
    EasyMock.expect(discoParty.getForNodeRole(INGEST_ROLE)).andReturn(discoBall).anyTimes();
    EasyMock.expect(discoBall.getAllNodes()).andReturn(ImmutableList.of()).anyTimes();
    expectedException.expect(RE.class);
    expectedException.expectMessage(
        StringUtils.format(
            "No [%s] servers available to query, make sure the service is running and announced",
            IngestService.SERVICE_NAME
        )
    );
  }

  private void replayAll()
  {
    EasyMock.replay(httpClient, discoParty, discoBall);
  }

  @FunctionalInterface
  interface IngestClientGetFunctionRunner<T>
  {
    T run() throws IOException;
  }
}
