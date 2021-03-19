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
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.ViewStateManagementConfig;
import io.imply.druid.sql.calcite.view.state.manager.sql.ViewStateSqlMetadataConnector;
import io.imply.druid.sql.calcite.view.state.manager.sql.ViewStateSqlMetadataConnectorTableConfig;
import io.imply.druid.sql.calcite.view.state.notifier.ViewStateNotifier;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class CoordinatorViewStateManagerTest
{
  SQLMetadataConnector connector;
  ViewStateSqlMetadataConnector viewStateConnector;
  ViewStateSqlMetadataConnectorTableConfig config;
  ObjectMapper smileMapper = TestHelper.makeSmileMapper();
  ViewStateNotifier viewStateNotifier;

  CoordinatorViewStateManager viewStateManager;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Before
  public void setup()
  {
    // using derby because mocking was too complex
    connector = derbyConnectorRule.getConnector();
    config = new ViewStateSqlMetadataConnectorTableConfig(true, null);
    viewStateConnector = new ViewStateSqlMetadataConnector(connector, config);
    viewStateConnector.init();
    viewStateNotifier = EasyMock.mock(ViewStateNotifier.class);

    viewStateManager = new CoordinatorViewStateManager(
        connector,
        config,
        new ViewStateManagementConfig(
            (long) 1000 * 60000, // extremely long polling period to avoid flakiness issues
            null,
            null,
            null,
            null,
            null,
            null
        ),
        viewStateNotifier,
        smileMapper
    );
  }

  /**
   * Simulates receiving a createView API call from the HTTP resource
   */
  @Test
  public void test_createViewFromAPICall() throws Exception
  {
    ImplyViewDefinition initialView = new ImplyViewDefinition(
        "initialView",
        null,
        "select * from baseDatasource",
        new DateTime(1000, DateTimeZone.UTC)
    );

    // initialize the DB with one view
    viewStateConnector.createView(initialView);
    viewStateManager.start();

    Map<String, ImplyViewDefinition> expectedViews = ImmutableMap.of(
        "initialView",
        initialView
    );
    byte[] expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);

    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    ImplyViewDefinition secondView = new ImplyViewDefinition(
        "secondView",
        null,
        "select * from baseDatasource2",
        new DateTime(1000, DateTimeZone.UTC)
    );
    viewStateManager.createView(secondView);

    expectedViews = ImmutableMap.of(
        "initialView",
        initialView,
        "secondView",
        secondView
    );
    expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);

    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    // try creating a view with the same name
    expectedException.expect(ViewAlreadyExistsException.class);
    expectedException.expectMessage("View secondView already exists");
    ImplyViewDefinition duplicateView = new ImplyViewDefinition(
        "secondView",
        null,
        "select * from baseDatasource3",
        new DateTime(1000, DateTimeZone.UTC)
    );
    viewStateManager.createView(duplicateView);
  }

  /**
   * Simulates receiving a createView API call from the HTTP resource
   */
  @Test
  public void test_deleteViewFromAPICall() throws Exception
  {
    ImplyViewDefinition initialView = new ImplyViewDefinition(
        "initialView",
        null,
        "select * from baseDatasource",
        new DateTime(1000, DateTimeZone.UTC)
    );

    // initialize the DB with one view
    viewStateConnector.createView(initialView);
    viewStateManager.start();

    Map<String, ImplyViewDefinition> expectedViews = ImmutableMap.of(
        "initialView",
        initialView
    );
    byte[] expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);
    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    // create a second view
    ImplyViewDefinition secondView = new ImplyViewDefinition(
        "secondView",
        null,
        "select * from baseDatasource2",
        new DateTime(1000, DateTimeZone.UTC)
    );
    viewStateManager.createView(secondView);
    expectedViews = ImmutableMap.of(
        "initialView",
        initialView,
        "secondView",
        secondView
    );
    expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);
    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    // try deleting a non-existent view
    // perhaps this should be an error instead of failing silently
    viewStateManager.deleteView("noSuchView", null);
    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    // delete both views
    viewStateManager.deleteView("initialView", null);
    expectedViews = ImmutableMap.of(
        "secondView",
        secondView
    );
    expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);
    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    viewStateManager.deleteView("secondView", null);
    expectedViews = ImmutableMap.of();
    expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);
    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());
  }

  /**
   * Simulates receiving an alterView API call from the HTTP resource
   */
  @Test
  public void test_alterViewFromAPICall() throws Exception
  {
    ImplyViewDefinition initialView = new ImplyViewDefinition(
        "initialView",
        null,
        "select * from baseDatasource",
        new DateTime(1000, DateTimeZone.UTC)
    );

    // initialize the DB with one view
    viewStateConnector.createView(initialView);
    viewStateManager.start();

    Map<String, ImplyViewDefinition> expectedViews = ImmutableMap.of(
        "initialView",
        initialView
    );
    byte[] expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);

    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    ImplyViewDefinition alteredView = new ImplyViewDefinition(
        "initialView",
        null,
        "select * from baseDatasource2",
        new DateTime(1000, DateTimeZone.UTC)
    );
    viewStateManager.alterView(alteredView);

    expectedViews = ImmutableMap.of(
        "initialView",
        alteredView
    );
    expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);

    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    // try altering a non-existent view
    // perhaps this should be an error instead of a silent failure
    ImplyViewDefinition alteredView2 = new ImplyViewDefinition(
        "noSuchView",
        null,
        "select * from baseDatasource2",
        new DateTime(1000, DateTimeZone.UTC)
    );
    viewStateManager.alterView(alteredView2);
    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());
  }

  /**
   * Simulates a metadata DB polling update
   */
  @Test
  public void test_updateAllStateFromMetadata() throws Exception
  {
    viewStateManager.start();

    Map<String, ImplyViewDefinition> expectedViews = ImmutableMap.of();
    byte[] expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);

    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());

    // insert two views directly into metadata
    ImplyViewDefinition initialView = new ImplyViewDefinition(
        "initialView",
        null,
        "select * from baseDatasource",
        new DateTime(1000, DateTimeZone.UTC)
    );
    ImplyViewDefinition secondView = new ImplyViewDefinition(
        "secondView",
        null,
        "select * from baseDatasource2",
        new DateTime(1000, DateTimeZone.UTC)
    );
    viewStateConnector.createView(initialView);
    viewStateConnector.createView(secondView);

    // Ideally we would test the polling for real, but we do a manual trigger here to avoid test flakiness issues.
    // The real polling can be tested with integration tests.
    viewStateManager.updateViewCacheFromMetadata();

    expectedViews = ImmutableMap.of(
        "initialView",
        initialView,
        "secondView",
        secondView
    );
    expectedViewsSerialized = smileMapper.writeValueAsBytes(expectedViews);

    Assert.assertEquals(expectedViews, viewStateManager.getViewState());
    Assert.assertArrayEquals(expectedViewsSerialized, viewStateManager.getViewStateSerialized());
  }
}
