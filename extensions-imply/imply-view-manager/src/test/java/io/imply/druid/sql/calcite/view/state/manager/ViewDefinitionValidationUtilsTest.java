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
import com.google.common.collect.ImmutableList;
import io.imply.druid.sql.calcite.view.ImplyViewManager;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ViewDefinitionValidationUtilsTest extends BaseCalciteQueryTest
{
  private ImplyViewManager viewManager;
  private PlannerFactory plannerFactory;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp2()
  {
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), jsonMapper)
    );

    this.viewManager = new ImplyViewManager(
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        CalciteTests.DRUID_VIEW_MACRO_FACTORY
    );

    SchemaPlus rootSchema = CalciteTests.createMockRootSchema(
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
  public void test_selectStar_allowed() throws Exception
  {
    String plan = getExplainPlan("SELECT * FROM druid.foo");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_withFilter_allowed() throws Exception
  {
    String plan = getExplainPlan("SELECT dim1 FROM druid.foo WHERE dim1 = 'test'");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_columnReplacedByConstantExpression_allowed() throws Exception
  {
    String plan = getExplainPlan("SELECT * FROM druid.foo WHERE druid.foo.dim1 = 'value'");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_selectStar_FilterOnTransformedColumn_allowed() throws Exception
  {
    String plan = getExplainPlan("SELECT * FROM druid.foo WHERE druid.foo.dim1 = UPPER(dim1)");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_selectDim1_FilterOnTransformedColumn_allowed() throws Exception
  {
    String plan = getExplainPlan("SELECT dim1 FROM druid.foo WHERE UPPER(dim1) = 'TEST'");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_selectTransformedColumn_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Transformations cannot be applied to projected columns in view definitions, columns[dim1]");

    String plan = getExplainPlan("SELECT UPPER(dim1) FROM druid.foo WHERE UPPER(dim1) = 'TEST'");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_selectTransformedMultipleColumn_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Transformations cannot be applied to projected columns in view definitions, columns[dim2, dim1]");

    String plan = getExplainPlan("SELECT CONCAT(dim1, dim2) FROM druid.foo");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_castType_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Transformations cannot be applied to projected columns in view definitions, columns[m2]");

    String plan = getExplainPlan("SELECT CAST(m2 AS VARCHAR) FROM druid.foo");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_SelectStarWithLimit_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("LIMIT cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT * FROM druid.foo LIMIT 10");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_SelectStarWithOffset_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("OFFSET cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT * FROM druid.foo OFFSET 5");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_SelectStarWithOrderBy_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("ORDER BY cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT * FROM druid.foo ORDER BY __time");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_SelectStarWithOrderByAsc_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("ORDER BY cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT * FROM druid.foo ORDER BY __time ASC");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_aggregation_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Aggregations cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT dim1, COUNT(*) FROM druid.foo GROUP BY dim1");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_LookupDatasource_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Lookup datasources cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT * FROM lookup.lookyloo");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_SubqueryAsFilterInnerJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT dim1, dim2 FROM druid.foo WHERE dim2 IN (SELECT dim2 FROM druid.foo)");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_SubQueryWithLimit_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Subqueries cannot be used in view definitions");

    String plan = getExplainPlan("SELECT COUNT(*) AS cnt FROM ( SELECT * FROM druid.foo LIMIT 10 ) tmpA");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_selfJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    String plan = getExplainPlan("SELECT COUNT(*) FROM druid.foo x, druid.foo y");
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_leftJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    String plan = getExplainPlan(
        "SELECT lookyloo.k, COUNT(*) FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k " +
        "WHERE lookyloo.v = '123' GROUP BY lookyloo.k"
    );
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_rightJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    String plan = getExplainPlan(
        "SELECT dim1, lookyloo.* FROM foo RIGHT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n" +
        "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL"
    );
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_topLevelUnion_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("UNION ALL cannot be used in view definitions.");

    String plan = getExplainPlan(
        "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo"
    );
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_tableLevelUnion_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("UNION ALL cannot be used in view definitions.");

    String plan = getExplainPlan(
        "SELECT dim1, COUNT(*) FROM (SELECT dim1, m1 FROM foo UNION ALL SELECT dim1, m1 FROM foo2) GROUP BY dim1"
    );
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_informationSchema_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Only queries on Druid datasources can be used as view definitions.");

    String plan = getExplainPlan(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_JOINABLE, IS_BROADCAST\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')"
    );
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void test_systemTables_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Only queries on Druid datasources can be used as view definitions.");

    String plan = getExplainPlan(
        "SELECT * from sys.segments"
    );
    ViewDefinitionValidationUtils.validateQueryPlan(plan, jsonMapper);
  }

  @Test
  public void testViewOnView() throws Exception
  {
    // create a view, the view query should work
    viewManager.createView(
        plannerFactory,
        "bview",
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
    );


    // create a view, the view query should work
    viewManager.createView(
        plannerFactory,
        "cview",
        "SELECT * FROM view.bview"
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(querySegmentSpec(Intervals.of("2000-01-02/2002")))
                                  .granularity(Granularities.ALL)
                                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                                  .context(TIMESERIES_CONTEXT_DEFAULT)
                                  .build();

    testQuery(
        "SELECT * FROM view.cview",
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
        "SELECT * FROM view.cview",
        ImmutableList.of(
            query
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  public String getExplainPlan(final String sql) throws Exception
  {
    return (String) getResults(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        "EXPLAIN PLAN FOR " + sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    ).get(0)[0];
  }
}
