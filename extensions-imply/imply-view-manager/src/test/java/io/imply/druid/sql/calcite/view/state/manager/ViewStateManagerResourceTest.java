/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource.ViewDefinitionRequest;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class ViewStateManagerResourceTest
{
  static {
    NullHandling.initializeForTests();
  }

  public static final String SOME_VIEW = "some_view";
  public static final String SOME_SQL = "SELECT * FROM druid.some_table WHERE druid.some_table.some_column = 'value'";

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory());

  public String someSQLExplainResponse;

  private HttpServletRequest req;
  private ViewStateManager viewStateManager;
  private ViewStateManagerResource viewStateManagerResource;
  private HttpClient httpClient;

  public static String buildSomeSQLExplainResponse()
  {
    String plan = "DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"some_table\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"'value'\",\"outputType\":\"STRING\"}],\"resultFormat\":\"compactedList\",\"batchSize\":20480,\"order\":\"none\",\"filter\":{\"type\":\"selector\",\"dimension\":\"some_column\",\"value\":\"value\",\"extractionFn\":null},\"columns\":[\"__time\",\"v0\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\"},\"descending\":false,\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, v0:STRING}])";
    String resources = "[{\"name\":\"some_table\",\"type\":\"DATASOURCE\"}]";
    List<Map<String, Object>> response = ImmutableList.of(
        ImmutableMap.of(
            "PLAN", plan,
            "RESOURCES", resources
        )
    );
    try {
      return JSON_MAPPER.writeValueAsString(response);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup()
  {
    JSON_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
    );
    req = EasyMock.createMock(HttpServletRequest.class);
    httpClient = EasyMock.createMock(HttpClient.class);

    List<DiscoveryDruidNode> nodeList = ImmutableList.of(
        new DiscoveryDruidNode(
            new DruidNode(
                "broker",
                "testhostname",
                false,
                9000,
                null,
                true,
                false
            ),
            NodeRole.BROKER,
            ImmutableMap.of()
        )
    );

    DruidNodeDiscoveryProvider discoveryProvider = new DruidNodeDiscoveryProvider()
    {
      @Override
      public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
      {
        return new BooleanSupplier()
        {
          @Override
          public boolean getAsBoolean()
          {
            return true;
          }
        };
      }

      @Override
      public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
      {
        return new DruidNodeDiscovery()
        {
          @Override
          public Collection<DiscoveryDruidNode> getAllNodes()
          {
            return nodeList;
          }

          @Override
          public void registerListener(Listener listener)
          {

          }
        };
      }
    };

    viewStateManager = EasyMock.createMock(ViewStateManager.class);
    viewStateManagerResource = new ViewStateManagerResource(
        new ViewStateManagementConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ),
        viewStateManager,
        discoveryProvider,
        httpClient,
        JSON_MAPPER
    );
    someSQLExplainResponse = buildSomeSQLExplainResponse();
  }

  @Test
  public void testCreateView()
  {
    expectHttpClientRequest(HttpResponseStatus.OK, someSQLExplainResponse);
    EasyMock.expect(viewStateManager.createView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(1).once();
    EasyMock.replay(req, httpClient, viewStateManager);
    Response response = viewStateManagerResource.createView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.CREATED.getCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
    Assert.assertTrue(response.getMetadata().containsKey("Location"));
    verifyAll();
  }

  @Test
  public void testAlterView()
  {
    expectHttpClientRequest(HttpResponseStatus.OK, someSQLExplainResponse);
    EasyMock.expect(viewStateManager.alterView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(1).once();
    EasyMock.replay(req, httpClient, viewStateManager);
    Response response = viewStateManagerResource.alterView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
    verifyAll();
  }

  @Test
  public void testAlterViewNotExist()
  {
    Map<String, Object> errorEntity = ImmutableMap.of("error", "View[some_view] not found.");

    expectHttpClientRequest(HttpResponseStatus.OK, someSQLExplainResponse);
    EasyMock.expect(viewStateManager.alterView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(0).once();
    EasyMock.replay(req, httpClient, viewStateManager);
    Response response = viewStateManagerResource.alterView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());
    verifyAll();
  }

  @Test
  public void testDeleteView()
  {
    EasyMock.expect(viewStateManager.deleteView(SOME_VIEW, null)).andReturn(1).once();
    replayAll();
    Response response = viewStateManagerResource.deleteView(
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
    verifyAll();
  }

  @Test
  public void testDeleteViewNotExist()
  {
    Map<String, Object> errorEntity = ImmutableMap.of("error", "View[some_view] not found.");

    EasyMock.expect(viewStateManager.deleteView(SOME_VIEW, null)).andReturn(0).once();
    replayAll();
    Response response = viewStateManagerResource.deleteView(
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());
    verifyAll();
  }

  @Test
  public void testGetViewsJson()
  {
    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, new ImplyViewDefinition(SOME_VIEW, SOME_SQL)
    );

    EasyMock.expect(viewStateManager.getViewState()).andReturn(views).once();
    EasyMock.expect(req.getHeader(HttpHeaders.Names.ACCEPT)).andReturn(MediaType.APPLICATION_JSON).once();

    replayAll();

    Response response = viewStateManagerResource.getViews(req);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertEquals(views, response.getEntity());

    verifyAll();
  }

  @Test
  public void testGetViewsCacheSmile() throws IOException
  {
    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, new ImplyViewDefinition(SOME_VIEW, SOME_SQL)
    );
    final byte[] bytes = SMILE_MAPPER.writeValueAsBytes(views);

    EasyMock.expect(viewStateManager.getViewStateSerialized()).andReturn(bytes).once();
    EasyMock.expect(req.getHeader(HttpHeaders.Names.ACCEPT))
            .andReturn(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .once();

    replayAll();

    Response response = viewStateManagerResource.getViews(req);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ((StreamingOutput) response.getEntity()).write(baos);
    Assert.assertArrayEquals(bytes, baos.toByteArray());

    verifyAll();
  }

  @Test
  public void testGetSingleViewJson()
  {
    ImplyViewDefinition someViewDefinition = new ImplyViewDefinition(SOME_VIEW, SOME_SQL);

    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, someViewDefinition
    );

    EasyMock.expect(viewStateManager.getViewState()).andReturn(views).once();

    replayAll();

    Response response = viewStateManagerResource.getSingleView(SOME_VIEW, req);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertEquals(someViewDefinition, response.getEntity());

    verifyAll();
  }


  @Test
  public void testBadNames()
  {
    String badName = "bad/name";
    Map<String, Object> errorEntity = ImmutableMap.of("error", "view cannot contain the '/' character.");

    Response response = viewStateManagerResource.createView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        badName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    response = viewStateManagerResource.alterView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        badName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    response = viewStateManagerResource.deleteView(
        badName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    response = viewStateManagerResource.getSingleView(
        badName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    response = viewStateManagerResource.getViewLoadStatus(
        badName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());
  }

  @Test
  public void testBadViewDefinition()
  {
    String viewName = "invalidView";
    Map<String, Object> errorEntity = ImmutableMap.of(
        "error",
        "Invalid view definition: SELECT * FROM druid.some_table WHERE druid.some_table.some_column = 'value'"
    );

    expectHttpClientRequest(HttpResponseStatus.BAD_REQUEST, null);
    expectHttpClientRequest(HttpResponseStatus.BAD_REQUEST, null);
    EasyMock.replay(req, httpClient, viewStateManager);

    Response response = viewStateManagerResource.createView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        viewName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    response = viewStateManagerResource.alterView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        viewName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());
  }

  @Test
  public void testGetViewLoadStatus_fresh() throws Exception
  {
    DateTime coordinatorTime = DateTimes.utc(10);

    ImplyViewDefinition someViewDefinition = new ImplyViewDefinition(SOME_VIEW, null, SOME_SQL, coordinatorTime);

    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, someViewDefinition
    );

    String brokerName = "testhostname:9000";

    // broker view has same modified time as coordinator view
    ViewStateManagerResource.ViewLoadStatusResponse expectedResponse =
        new ViewStateManagerResource.ViewLoadStatusResponse(
            ImmutableList.of(brokerName),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()
        );
    testGetViewLoadStatusHelper(
        views,
        HttpResponseStatus.OK,
        someViewDefinition,
        HttpResponseStatus.OK,
        expectedResponse
    );
  }

  @Test
  public void testGetViewLoadStatus_freshest() throws Exception
  {
    DateTime coordinatorTime = DateTimes.utc(10);
    DateTime afterCoordinatorTime = DateTimes.utc(11);

    ImplyViewDefinition someViewDefinition = new ImplyViewDefinition(SOME_VIEW, null, SOME_SQL, coordinatorTime);
    ImplyViewDefinition freshestViewDefinition = new ImplyViewDefinition(
        SOME_VIEW,
        null,
        SOME_SQL,
        afterCoordinatorTime
    );

    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, someViewDefinition
    );

    String brokerName = "testhostname:9000";

    // broker view has newer modified time than coordinator view
    ViewStateManagerResource.ViewLoadStatusResponse expectedResponse =
        new ViewStateManagerResource.ViewLoadStatusResponse(
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(brokerName),
            ImmutableList.of()
        );
    testGetViewLoadStatusHelper(
        views,
        HttpResponseStatus.OK,
        freshestViewDefinition,
        HttpResponseStatus.OK,
        expectedResponse
    );
  }

  @Test
  public void testGetViewLoadStatus_stale() throws Exception
  {
    DateTime coordinatorTime = DateTimes.utc(10);
    DateTime beforeCoordinatorTime = DateTimes.utc(9);

    ImplyViewDefinition someViewDefinition = new ImplyViewDefinition(SOME_VIEW, null, SOME_SQL, coordinatorTime);
    ImplyViewDefinition staleViewDefinition = new ImplyViewDefinition(
        SOME_VIEW,
        null,
        SOME_SQL,
        beforeCoordinatorTime
    );

    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, someViewDefinition
    );

    String brokerName = "testhostname:9000";

    // broker view has older modified time than coordinator view
    ViewStateManagerResource.ViewLoadStatusResponse expectedResponse =
        new ViewStateManagerResource.ViewLoadStatusResponse(
            ImmutableList.of(),
            ImmutableList.of(brokerName),
            ImmutableList.of(),
            ImmutableList.of()
        );
    testGetViewLoadStatusHelper(
        views,
        HttpResponseStatus.OK,
        staleViewDefinition,
        HttpResponseStatus.OK,
        expectedResponse
    );
  }

  @Test
  public void testGetViewLoadStatus_unknown() throws Exception
  {
    DateTime coordinatorTime = DateTimes.utc(10);

    ImplyViewDefinition someViewDefinition = new ImplyViewDefinition(SOME_VIEW, null, SOME_SQL, coordinatorTime);
    final Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        SOME_VIEW, someViewDefinition
    );

    String brokerName = "testhostname:9000";

    // error fetching view definition from broker
    ViewStateManagerResource.ViewLoadStatusResponse expectedResponse =
        new ViewStateManagerResource.ViewLoadStatusResponse(
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(brokerName)
        );
    testGetViewLoadStatusHelper(
        views,
        HttpResponseStatus.INTERNAL_SERVER_ERROR,
        null,
        HttpResponseStatus.OK,
        expectedResponse
    );
  }

  private void testGetViewLoadStatusHelper(
      Map<String, ImplyViewDefinition> coordinatorViews,
      HttpResponseStatus responseStatusFromBroker,
      ImplyViewDefinition brokerViewDefinition,
      HttpResponseStatus expectedResponseStatus,
      ViewStateManagerResource.ViewLoadStatusResponse expectedResponse
  ) throws Exception
  {
    expectHttpClientRequest(responseStatusFromBroker, JSON_MAPPER.writeValueAsString(brokerViewDefinition));

    EasyMock.expect(viewStateManager.getViewState()).andReturn(coordinatorViews).once();
    EasyMock.replay(req, httpClient, viewStateManager);

    Response response = viewStateManagerResource.getViewLoadStatus(SOME_VIEW, req);
    Assert.assertEquals(expectedResponseStatus.getCode(), response.getStatus());
    Assert.assertEquals(expectedResponse, response.getEntity());

    EasyMock.verify(req, httpClient, viewStateManager);

    EasyMock.reset(req, httpClient, viewStateManager);
  }

  @Test
  public void testViewDefinitionValidationInternalError()
  {
    String viewName = "testview";
    Map<String, Object> errorEntity = ImmutableMap.of(
        "error",
        StringUtils.format(
            "Received non-OK response while validating view definition[%s], status[500 Internal Server Error]",
            SOME_SQL
        )
    );

    expectHttpClientRequest(HttpResponseStatus.INTERNAL_SERVER_ERROR, null);
    expectHttpClientRequest(HttpResponseStatus.INTERNAL_SERVER_ERROR, null);
    EasyMock.replay(req, httpClient, viewStateManager);

    Response response = viewStateManagerResource.createView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        viewName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    response = viewStateManagerResource.alterView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        viewName,
        req
    );
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatus());
    Assert.assertEquals(errorEntity, response.getEntity());

    EasyMock.verify(req, httpClient, viewStateManager);
  }

  @Test
  public void testSerdeViewDefinitionRequest() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final ViewDefinitionRequest request = new ViewDefinitionRequest("select * from test");
    final String json = mapper.writeValueAsString(request);
    System.err.println(json);
    final ViewDefinitionRequest fromJson = mapper.readValue(json, ViewDefinitionRequest.class);
    Assert.assertEquals(request, fromJson);
  }

  @Test
  public void testSerdeViewLoadStatusResponse() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final ViewStateManagerResource.ViewLoadStatusResponse request = new ViewStateManagerResource.ViewLoadStatusResponse(
        ImmutableList.of("hello:1234"),
        ImmutableList.of("world:1234"),
        ImmutableList.of("foo:1234"),
        ImmutableList.of("bar:1234")
    );
    final String json = mapper.writeValueAsString(request);
    System.err.println(json);
    final ViewStateManagerResource.ViewLoadStatusResponse fromJson = mapper.readValue(
        json,
        ViewStateManagerResource.ViewLoadStatusResponse.class
    );
    Assert.assertEquals(request, fromJson);
  }

  @Test
  public void test_ViewDefinitionRequest_equals()
  {
    EqualsVerifier.forClass(ViewStateManagerResource.ViewDefinitionRequest.class)
                  .usingGetClass()
                  .withNonnullFields("viewSql")
                  .verify();
  }

  @Test
  public void test_ViewLoadStatusResponse_equals()
  {
    EqualsVerifier.forClass(ViewStateManagerResource.ViewLoadStatusResponse.class)
                  .usingGetClass()
                  .withNonnullFields("fresh", "stale", "unknown")
                  .verify();
  }

  private void replayAll()
  {
    EasyMock.replay(req, viewStateManager);
  }

  private void verifyAll()
  {
    EasyMock.verify(req, viewStateManager);
  }

  private void expectHttpClientRequest(
      HttpResponseStatus expectedResponseStatus,
      String responseContent
  )
  {
    ListenableFuture<StatusResponseHolder> future = new ListenableFuture<StatusResponseHolder>()
    {
      @Override
      public void addListener(Runnable listener, Executor executor)
      {
        executor.execute(listener);
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public StatusResponseHolder get()
      {
        return new StatusResponseHolder(
            expectedResponseStatus,
            new StringBuilder(responseContent == null ? "" : responseContent)
        );
      }

      @Override
      public StatusResponseHolder get(long timeout, TimeUnit unit)
      {
        return new StatusResponseHolder(
            expectedResponseStatus,
            new StringBuilder(responseContent == null ? "" : responseContent)
        );
      }
    };

    EasyMock.expect(
        httpClient.go(
            EasyMock.anyObject(Request.class),
            EasyMock.anyObject(StatusResponseHandler.class),
            EasyMock.anyObject(Duration.class)
        )).andReturn(future).once();
  }

}
