/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerFixture;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

public class ImplyViewManagerTest extends BaseCalciteQueryTest
{
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
  public void populateViews(ViewManager viewManager, PlannerFactory plannerFactory)
  {
    // None of the standard, default views.
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
  public void testCreateAndDeleteView()
  {
    PlannerFixture plannerFixture = plannerFixture();
    // create a view, the view query should work
    plannerFixture.viewManager().createView(
        plannerFixture.plannerFactory(),
        "bview",
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(querySegmentSpec(Intervals.of("2000-01-02/2002")))
                                  .granularity(Granularities.ALL)
                                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                                  .context(QUERY_CONTEXT_DEFAULT)
                                  .build();

    testBuilder()
      .plannerFixture(plannerFixture)
      .sql("SELECT * FROM view.bview")
      .expectedQuery(query)
      .expectedResults(
         ImmutableList.of(
            new Object[]{5L}
         )
       )
      .run();

    // Drop the view, the view query should fail
    expectedException.expect(DruidException.class);
    expectedException.expectMessage("Object 'bview' not found within 'view'");

    plannerFixture.viewManager().dropView("bview");
    testBuilder()
      .plannerFixture(plannerFixture)
      .sql("SELECT * FROM view.bview")
      .expectedQuery(query)
      .expectedResults(ImmutableList.of(new Object[]{5L}))
      .run();
  }

  @Test
  public void testTemporaryViewLoader() throws IOException
  {
    PlannerFixture plannerFixture = plannerFixture();
    ImplyViewCacheUpdateMessage message = new ImplyViewCacheUpdateMessage(
        ImmutableMap.of(
            "cloned_foo",
            new ImplyViewDefinition(
                "cloned_foo",
                "SELECT * FROM foo"
            ),
            "filter_bar",
            new ImplyViewDefinition(
                "filter_bar",
                "SELECT * FROM bar WHERE col = 'a'"
            )
        )
    );

    TemporaryViewDefinitionLoader loader = new TemporaryViewDefinitionLoader(
        queryFramework().queryJsonMapper(),
        plannerFixture.plannerFactory(),
        plannerFixture.viewManager()
    );

    File testDefs = File.createTempFile("viewloadertest", null);
    loader.setFileToUse(testDefs);

    queryFramework().queryJsonMapper().writeValue(testDefs, message);

    loader.start();
    loader.stop();
  }
}
