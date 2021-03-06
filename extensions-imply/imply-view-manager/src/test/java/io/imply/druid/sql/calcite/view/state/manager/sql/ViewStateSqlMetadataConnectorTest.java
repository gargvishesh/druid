/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager.sql;

import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.manager.ViewAlreadyExistsException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class ViewStateSqlMetadataConnectorTest
{
  private static final String VIEW_NAME = "some_view";

  SQLMetadataConnector connector;
  ViewStateSqlMetadataConnector viewStateConnector;
  ViewStateSqlMetadataConnectorTableConfig config;

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
  }

  @Test
  public void createViewsTable()
  {
    Assert.assertTrue(connector.retryWithHandle((handle) -> connector.tableExists(handle, config.getViewsTable())));
  }

  @Test
  public void testCannotInsertTwice()
  {
    expectedException.expect(ViewAlreadyExistsException.class);
    expectedException.expectMessage(StringUtils.format("View %s already exists", VIEW_NAME));
    // staring from 0
    Assert.assertEquals(0, viewStateConnector.getViews().size());

    // add a new view
    ImplyViewDefinition def = new ImplyViewDefinition(
        VIEW_NAME,
        null,
        "SELECT * FROM table WHERE table.column1 = 'value'",
        DateTimes.nowUtc()
    );

    Assert.assertEquals(1, viewStateConnector.createView(def));
    viewStateConnector.createView(def);
  }

  @Test
  public void testCannotInsertTwiceNamespaces()
  {
    expectedException.expect(ViewAlreadyExistsException.class);
    expectedException.expectMessage(StringUtils.format("View some_namespace.%s already exists", VIEW_NAME));
    // staring from 0
    Assert.assertEquals(0, viewStateConnector.getViews().size());

    // add a new view
    ImplyViewDefinition def = new ImplyViewDefinition(
        VIEW_NAME,
        "some_namespace",
        "SELECT * FROM table WHERE table.column1 = 'value'",
        DateTimes.nowUtc()
    );

    Assert.assertEquals(1, viewStateConnector.createView(def));
    viewStateConnector.createView(def);
  }

  @Test
  public void testViewCreateUpdateDelete()
  {
    // staring from 0
    Assert.assertEquals(0, viewStateConnector.getViews().size());

    // add a new view
    ImplyViewDefinition def = new ImplyViewDefinition(
        VIEW_NAME,
        null,
        "SELECT * FROM table WHERE table.column1 = 'value'",
        DateTimes.nowUtc()
    );

    Assert.assertEquals(1, viewStateConnector.createView(def));

    List<ImplyViewDefinition> views = viewStateConnector.getViews();
    Assert.assertEquals(1, views.size());
    Assert.assertEquals(def, views.get(0));

    // update that view
    ImplyViewDefinition newDef = def.withSql(
        "SELECT * FROM table WHERE table.column1 = 'value' AND table.column2 = 'other value'"
    );
    Assert.assertEquals(1, viewStateConnector.alterView(newDef));

    views = viewStateConnector.getViews();
    Assert.assertEquals(1, views.size());
    Assert.assertEquals(newDef, views.get(0));

    // now delete it, nothing remains
    viewStateConnector.deleteView(newDef);
    views = viewStateConnector.getViews();
    Assert.assertEquals(0, views.size());
  }
}
