/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.ImplyViewManager;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.ViewStateUtils;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerFixture;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class BrokerViewStateListenerTest extends BaseCalciteQueryTest
{
  private static final ServiceEmitter EMITTER = new ServiceEmitter(
      "service",
      "host",
      new NoopEmitter()
  );

  private final ObjectMapper smileMapper = TestHelper.makeSmileMapper();
  private DruidLeaderClient druidLeaderClient;
  private File tempDir;
  private ViewStateManagementConfig stateManagementConfig;
  private BrokerViewStateListener viewStateListener;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Override
  public ViewManager createViewManager()
  {
    return new ImplyViewManager(
        SqlTestFramework.DRUID_VIEW_MACRO_FACTORY
    );
  }

  @Override
  public void finalizePlanner(PlannerFixture plannerFixture)
  {
    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);

    try {
      tempDir = temporaryFolder.newFolder();
    }
    catch (IOException e) {
      throw new ISE(e, "Could not create temp file");
    }
    stateManagementConfig = new ViewStateManagementConfig(
        1000 * 60000L, // extremely long polling period to avoid flakiness issues
        null,
        tempDir.getAbsolutePath(),
        1,
        null,
        null,
        1000 * 60000L, // extremely long polling period to avoid flakiness issues
        null
    );
    viewStateListener = new BrokerViewStateListener(
        stateManagementConfig,
        plannerFixture.viewManager(),
        plannerFixture.plannerFactory(),
        queryFramework().queryJsonMapper(),
        druidLeaderClient
    );
  }

  @Before
  public void setUp2()
  {
    EmittingLogger.registerEmitter(EMITTER);
    EMITTER.start();
  }

  private PlannerFixture plannerFixture()
  {
    return queryFramework().plannerFixture(
        this,
        BaseCalciteQueryTest.PLANNER_CONFIG_DEFAULT,
        new AuthConfig()
    );
  }

  @Test
  public void testStart_andUpdateFromCoordinator() throws Exception
  {
    // Force the planner fixture to be created, along with our added listeners.
    plannerFixture();
    Request request = EasyMock.createMock(Request.class);
    EasyMock.expect(request.addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE))
            .andReturn(request).anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid-ext/view-manager/v1/views"))
            .andReturn(request).anyTimes();

    Map<String, ImplyViewDefinition> viewDefinitionMap = ImmutableMap.of(
        "testView",
        new ImplyViewDefinition(
            "testView",
            "select * from baseDatasource"
        ),
        "testView2",
        new ImplyViewDefinition(
            "testView2",
            "select * from baseDatasource2"
        )
    );
    byte[] viewDefinitionMapSerialized = smileMapper.writeValueAsBytes(viewDefinitionMap);

    BytesFullResponseHolder bytesFullResponseHolder = EasyMock.createMock(BytesFullResponseHolder.class);
    EasyMock.expect(bytesFullResponseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(bytesFullResponseHolder.getContent()).andReturn(viewDefinitionMapSerialized).anyTimes();

    EasyMock.expect(druidLeaderClient.go(
        EasyMock.anyObject(Request.class),
        EasyMock.anyObject(BytesFullResponseHandler.class)
    )).andReturn(bytesFullResponseHolder).anyTimes();

    EasyMock.replay(request, bytesFullResponseHolder, druidLeaderClient);

    viewStateListener.start();

    Assert.assertEquals(viewDefinitionMap, viewStateListener.getViewState());
    Assert.assertArrayEquals(viewDefinitionMapSerialized, viewStateListener.getViewStateSerialized());
    Assert.assertEquals(
        viewDefinitionMap,
        smileMapper.readValue(
            viewStateListener.getViewStateSnapshotFile(),
            ViewStateUtils.VIEW_MAP_TYPE_REFERENCE
        )
    );

    // reset the mocks, we're going to simulate the broker polling the coordinator and getting an updated map
    EasyMock.reset(bytesFullResponseHolder, druidLeaderClient);

    Map<String, ImplyViewDefinition> viewDefinitionMap2 = ImmutableMap.of(
        "testView",
        new ImplyViewDefinition(
            "testView",
            "select * from baseDatasource"
        ),
        "testView3",
        new ImplyViewDefinition(
            "testView3",
            "select * from baseDatasource3"
        )
    );
    byte[] viewDefinitionMapSerialized2 = smileMapper.writeValueAsBytes(viewDefinitionMap2);
    EasyMock.expect(bytesFullResponseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(bytesFullResponseHolder.getContent()).andReturn(viewDefinitionMapSerialized2).anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid-ext/view-manager/v1/views"))
            .andReturn(request).anyTimes();
    EasyMock.expect(druidLeaderClient.go(
        EasyMock.anyObject(Request.class),
        EasyMock.anyObject(BytesFullResponseHandler.class)
    )).andReturn(bytesFullResponseHolder).anyTimes();

    EasyMock.replay(bytesFullResponseHolder, druidLeaderClient);

    // Ideally we'd test this with a real polling loop but manually calling this is more deterministic, to avoid
    // test flakiness issuess. The real polling loop can be tested with integration tests.
    viewStateListener.tryUpdateViewStateFromCoordinator();

    Assert.assertEquals(viewDefinitionMap2, viewStateListener.getViewState());
    Assert.assertArrayEquals(viewDefinitionMapSerialized2, viewStateListener.getViewStateSerialized());
    Assert.assertEquals(
        viewDefinitionMap2,
        smileMapper.readValue(
            viewStateListener.getViewStateSnapshotFile(),
            ViewStateUtils.VIEW_MAP_TYPE_REFERENCE
        )
    );

    // now update as if receiving a push update from the coordinator
    Map<String, ImplyViewDefinition> viewDefinitionMap3 = ImmutableMap.of(
        "testView4",
        new ImplyViewDefinition(
            "testView",
            "select * from baseDatasource4"
        ),
        "testView5",
        new ImplyViewDefinition(
            "testView5",
            "select * from baseDatasource5"
        ),
        "testView6",
        new ImplyViewDefinition(
            "testView5",
            "select * from baseDatasource5"
        )
    );
    byte[] viewDefinitionMapSerialized3 = smileMapper.writeValueAsBytes(viewDefinitionMap3);
    viewStateListener.setViewState(viewDefinitionMapSerialized3);

    Assert.assertEquals(viewDefinitionMap3, viewStateListener.getViewState());
    Assert.assertArrayEquals(viewDefinitionMapSerialized3, viewStateListener.getViewStateSerialized());
    Assert.assertEquals(
        viewDefinitionMap3,
        smileMapper.readValue(
            viewStateListener.getViewStateSnapshotFile(),
            ViewStateUtils.VIEW_MAP_TYPE_REFERENCE
        )
    );

    viewStateListener.stop();
  }

  @Test
  public void testStart_coordinatorDown_loadFromDisk() throws Exception
  {
    // Force the planner fixture to be created, along with our added listeners.
    plannerFixture();
    Request request = EasyMock.createMock(Request.class);
    EasyMock.expect(request.addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE))
            .andReturn(request).anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid-ext/view-manager/v1/views"))
            .andReturn(request).anyTimes();

    Map<String, ImplyViewDefinition> viewDefinitionMap = ImmutableMap.of(
        "testView",
        new ImplyViewDefinition(
            "testView",
            "select * from baseDatasource"
        )
    );
    byte[] viewDefinitionMapSerialized = smileMapper.writeValueAsBytes(viewDefinitionMap);

    // write the snapshot to disk before starting
    viewStateListener.writeViewStateToDisk(viewDefinitionMapSerialized);

    EasyMock.expect(druidLeaderClient.go(
        EasyMock.anyObject(Request.class),
        EasyMock.anyObject(BytesFullResponseHandler.class)
    )).andThrow(new RuntimeException("coordinator is inaccessible")).anyTimes();

    EasyMock.replay(request, druidLeaderClient);

    viewStateListener.start();

    Assert.assertEquals(viewDefinitionMap, viewStateListener.getViewState());
    Assert.assertArrayEquals(viewDefinitionMapSerialized, viewStateListener.getViewStateSerialized());
    Assert.assertEquals(
        viewDefinitionMap,
        smileMapper.readValue(
            viewStateListener.getViewStateSnapshotFile(),
            ViewStateUtils.VIEW_MAP_TYPE_REFERENCE
        )
    );

    viewStateListener.stop();
  }
}