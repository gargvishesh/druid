/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class ViewStateManagerResourceTest
{
  public static final String SOME_VIEW = "some_view";
  public static final String SOME_SQL = "SELECT * FROM druid.some_table WHERE druid.some_table.some_column = 'value'";

  private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory());

  private HttpServletRequest req;
  private ViewStateManager viewStateManager;
  private ViewStateManagerResource viewStateManagerResource;

  @Before
  public void setup()
  {
    req = EasyMock.createMock(HttpServletRequest.class);
    viewStateManager = EasyMock.createMock(ViewStateManager.class);
    viewStateManagerResource = new ViewStateManagerResource(viewStateManager);
  }

  @Test
  public void testCreateView()
  {
    EasyMock.expect(viewStateManager.createView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(1).once();
    replayAll();
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
    EasyMock.expect(viewStateManager.alterView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(1).once();
    replayAll();
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
    EasyMock.expect(viewStateManager.alterView(EasyMock.anyObject(ImplyViewDefinition.class))).andReturn(0).once();
    replayAll();
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

  private void replayAll()
  {
    EasyMock.replay(req, viewStateManager);
  }

  private void verifyAll()
  {
    EasyMock.verify(req, viewStateManager);
  }
}
