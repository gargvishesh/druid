/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource.ViewDefinitionRequest;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
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
  public static final String SOME_VIEW = "some_view";
  public static final String SOME_SQL = "SELECT * FROM druid.some_table WHERE druid.some_table.some_column = 'value'";

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory());

  private HttpServletRequest req;
  private ViewStateManager viewStateManager;
  private ViewStateManagerResource viewStateManagerResource;
  private HttpClient httpClient;

  @Before
  public void setup()
  {
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
    viewStateManagerResource = new ViewStateManagerResource(viewStateManager, discoveryProvider, httpClient, JSON_MAPPER);
  }

  @Test
  public void testCreateView()
  {
    expectBrokerValidationCheck(HttpResponseStatus.OK);
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
    expectBrokerValidationCheck(HttpResponseStatus.OK);
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
    expectBrokerValidationCheck(HttpResponseStatus.OK);
    EasyMock.expect(viewStateManager.alterView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(0).once();
    EasyMock.replay(req, httpClient, viewStateManager);
    Response response = viewStateManagerResource.alterView(
        new ViewStateManagerResource.ViewDefinitionRequest(SOME_SQL),
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
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
    EasyMock.expect(viewStateManager.deleteView(SOME_VIEW, null)).andReturn(0).once();
    replayAll();
    Response response = viewStateManagerResource.deleteView(
        SOME_VIEW,
        req
    );
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
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
  }

  @Test
  public void testBadViewDefinition()
  {
    String viewName = "invalidView";
    Map<String, Object> errorEntity = ImmutableMap.of(
        "error",
        "Invalid view definition: SELECT * FROM druid.some_table WHERE druid.some_table.some_column = 'value'"
    );

    expectBrokerValidationCheck(HttpResponseStatus.BAD_REQUEST);
    expectBrokerValidationCheck(HttpResponseStatus.BAD_REQUEST);
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

    expectBrokerValidationCheck(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    expectBrokerValidationCheck(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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

  private void replayAll()
  {
    EasyMock.replay(req, viewStateManager);
  }

  private void verifyAll()
  {
    EasyMock.verify(req, viewStateManager);
  }

  private void expectBrokerValidationCheck(HttpResponseStatus expectedResponseStatus)
  {
    ListenableFuture<StatusResponseHolder> future = new ListenableFuture<StatusResponseHolder>()
    {
      @Override
      public void addListener(Runnable listener, Executor executor)
      {

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
        return new StatusResponseHolder(expectedResponseStatus, new StringBuilder());
      }

      @Override
      public StatusResponseHolder get(long timeout, TimeUnit unit)
      {
        return new StatusResponseHolder(expectedResponseStatus, new StringBuilder());
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
