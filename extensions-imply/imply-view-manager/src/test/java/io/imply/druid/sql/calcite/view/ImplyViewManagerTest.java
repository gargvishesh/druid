/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

public class ImplyViewManagerTest extends BaseCalciteQueryTest
{
  private ImplyViewManager viewManager;
  private PlannerFactory plannerFactory;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp2()
  {
    this.viewManager = new ImplyViewManager(
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        CalciteTests.DRUID_VIEW_MACRO_FACTORY
    );

    DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        conglomerate,
        walker,
        PLANNER_CONFIG_DEFAULT,
        viewManager,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );

    this.plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        PLANNER_CONFIG_DEFAULT,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        jsonMapper,
        CalciteTests.DRUID_SCHEMA_NAME
    );
  }

  @Override
  public SqlLifecycleFactory getSqlLifecycleFactory(
      PlannerConfig plannerConfig,
      DruidOperatorTable operatorTable,
      ExprMacroTable macroTable,
      AuthorizerMapper authorizerMapper,
      ObjectMapper objectMapper
  )
  {
    return CalciteTests.createSqlLifecycleFactory(plannerFactory);
  }

  @Test
  public void testCreateAndDeleteView() throws Exception
  {
    // create a view, the view query should work
    viewManager.createView(
        plannerFactory,
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

    testQuery(
        "SELECT * FROM view.bview",
        ImmutableList.of(
          query
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );

    // Drop the view, the view query should fail
    expectedException.expect(SqlPlanningException.class);
    expectedException.expectMessage("Object 'bview' not found within 'view'");

    viewManager.dropView("bview");
    testQuery(
        "SELECT * FROM view.bview",
        ImmutableList.of(
            query
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );

  }

  @Test
  public void test_temporary_view_loader() throws IOException
  {
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
        jsonMapper,
        plannerFactory,
        viewManager
    );

    File testDefs = File.createTempFile("viewloadertest", null);
    loader.setFileToUse(testDefs);

    jsonMapper.writeValue(testDefs, message);

    loader.start();
    loader.stop();
  }
}
