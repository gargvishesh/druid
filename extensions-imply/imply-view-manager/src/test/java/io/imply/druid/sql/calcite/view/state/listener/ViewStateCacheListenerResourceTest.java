/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResourceTest;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.Map;

public class ViewStateCacheListenerResourceTest
{
  private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory());

  private HttpServletRequest req;
  private ViewStateListener viewStateListener;
  private ViewStateListenerResource viewStateListenerResource;

  @Before
  public void setup()
  {
    req = EasyMock.createMock(HttpServletRequest.class);
    viewStateListener = EasyMock.createMock(ViewStateListener.class);
    viewStateListenerResource = new ViewStateListenerResource(viewStateListener);
  }

  @Test
  public void testUpdateCache() throws JsonProcessingException
  {
    Map<String, ImplyViewDefinition> views = ImmutableMap.of(
        ViewStateManagerResourceTest.SOME_VIEW,
        new ImplyViewDefinition(ViewStateManagerResourceTest.SOME_VIEW, ViewStateManagerResourceTest.SOME_SQL)
    );
    final byte[] bytes = SMILE_MAPPER.writeValueAsBytes(views);

    viewStateListener.setViewState(bytes);
    EasyMock.expectLastCall();

    replayAll();

    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    Response response = viewStateListenerResource.updateCache(input, req);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertNull(response.getEntity());

    verifyAll();
  }

  private void replayAll()
  {
    EasyMock.replay(req, viewStateListener);
  }

  private void verifyAll()
  {
    EasyMock.verify(req, viewStateListener);
  }
}
