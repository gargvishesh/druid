/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.sql.calcite.view.ImplyViewManager;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public void test_selectStar_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo");

    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selectStar_withoutNamespace_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM foo");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selectStart_withoutNamespace_viewWithSameName_allowed() throws Exception
  {
    viewManager.createView(
        plannerFactory,
        "foo",
        "SELECT * FROM druid.foo WHERE dim1 != 'aaaaa'"
    );

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM foo");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);

    // Make sure that we queried the datasource, not the view with the same name
    Assert.assertEquals(
        ImmutableSet.of(new Resource("foo", ResourceType.DATASOURCE)),
        planAndResources.getResources()
    );
  }

  @Test
  public void test_withFilter_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT dim1 FROM druid.foo WHERE dim1 = 'test'");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }


  @Test
  public void test_withFilter_withNewLine_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT dim1 FROM \n" + "druid.foo WHERE dim1 = '\\ntest\n'");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_columnReplacedByConstantExpression_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo WHERE druid.foo.dim1 = 'value'");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selectStar_FilterOnTransformedColumn_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo WHERE druid.foo.dim1 = UPPER(dim1)");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selectDim1_FilterOnTransformedColumn_allowed() throws Exception
  {
    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT dim1 FROM druid.foo WHERE UPPER(dim1) = 'TEST'");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selectTransformedColumn_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Transformations cannot be applied to projected columns in view definitions, columns[dim1]");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT UPPER(dim1) FROM druid.foo WHERE UPPER(dim1) = 'TEST'");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selectTransformedMultipleColumn_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Transformations cannot be applied to projected columns in view definitions, columns[dim2, dim1]");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT CONCAT(dim1, dim2) FROM druid.foo");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_castType_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Transformations cannot be applied to projected columns in view definitions, columns[m2]");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT CAST(m2 AS VARCHAR) FROM druid.foo");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_SelectStarWithLimit_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("LIMIT cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo LIMIT 10");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_SelectStarWithOffset_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("OFFSET cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo OFFSET 5");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_SelectStarWithOrderBy_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("ORDER BY cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo ORDER BY __time");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_SelectStarWithOrderByAsc_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("ORDER BY cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM druid.foo ORDER BY __time ASC");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_aggregation_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Aggregations cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT dim1, COUNT(*) FROM druid.foo GROUP BY dim1");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_LookupDatasource_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Lookup datasources cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT * FROM lookup.lookyloo");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_SubqueryAsFilterInnerJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT dim1, dim2 FROM druid.foo WHERE dim2 IN (SELECT dim2 FROM druid.foo)");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_SubQueryWithLimit_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Subqueries cannot be used in view definitions");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT COUNT(*) AS cnt FROM ( SELECT * FROM druid.foo LIMIT 10 ) tmpA");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_selfJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources =
        getExplainPlanAndResources("SELECT COUNT(*) FROM druid.foo x, druid.foo y");
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_leftJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT lookyloo.k, COUNT(*) FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k " +
        "WHERE lookyloo.v = '123' GROUP BY lookyloo.k"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_rightJoin_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Joins or subqueries in the WHERE clause cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT dim1, lookyloo.* FROM foo RIGHT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n" +
        "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_topLevelUnion_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("UNION ALL cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_tableLevelUnion_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("UNION ALL cannot be used in view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT dim1, COUNT(*) FROM (SELECT dim1, m1 FROM foo UNION ALL SELECT dim1, m1 FROM foo2) GROUP BY dim1"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_informationSchema_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Only queries on Druid datasources can be used as view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_JOINABLE, IS_BROADCAST\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_systemTables_denied() throws Exception
  {
    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("Only queries on Druid datasources can be used as view definitions.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT * from sys.segments"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_viewOnView_denied() throws Exception
  {
    viewManager.createView(
        plannerFactory,
        "bview",
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
    );

    expectedException.expect(ViewDefinitionValidationUtils.ClientValidationException.class);
    expectedException.expectMessage("View [bview] cannot be used within another view definition.");

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources = getExplainPlanAndResources(
        "SELECT * from view.bview"
    );
    ViewDefinitionValidationUtils.validateQueryPlanAndResources(planAndResources, jsonMapper);
  }

  @Test
  public void test_extractQueryPlanAndResources() throws Exception
  {
    viewManager.createView(
        plannerFactory,
        "bview",
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
    );

    List<String> queries = ImmutableList.of(
        "SELECT * FROM druid.foo",
        "SELECT * FROM foo",
        "SELECT * from view.bview",
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('SYSTEM_TABLE')",
        "SELECT lookyloo.k, COUNT(*) FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k WHERE lookyloo.v = '123' GROUP BY lookyloo.k"
    );

    ViewDefinitionValidationUtils.QueryPlanAndResources planAndResources;
    String responseContent;
    ViewDefinitionValidationUtils.QueryPlanAndResources newPlanAndResources;
    for (String query : queries) {
      planAndResources = getExplainPlanAndResources(query);
      responseContent = makeExplainResponseContentFromQueryPlanAndResources(planAndResources);
      newPlanAndResources = ViewDefinitionValidationUtils.getQueryPlanAndResourcesFromExplainResponse(
          responseContent,
          jsonMapper
      );
      Assert.assertEquals(planAndResources, newPlanAndResources);
    }
  }

  @Test
  public void test_QueryPlanAndResources_equals()
  {
    EqualsVerifier.forClass(ViewDefinitionValidationUtils.QueryPlanAndResources.class)
                  .usingGetClass()
                  .withNonnullFields("queryPlan", "resources")
                  .verify();
  }


  public String makeExplainResponseContentFromQueryPlanAndResources(
      ViewDefinitionValidationUtils.QueryPlanAndResources queryPlanAndResources
  )
  {
    try {
      List<Map<String, Object>> responseStruct = ImmutableList.of(
          ImmutableMap.of(
              "PLAN", queryPlanAndResources.getQueryPlan(),
              "RESOURCES", jsonMapper.writeValueAsString(queryPlanAndResources.getResources())
          )
      );
      return jsonMapper.writeValueAsString(responseStruct);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public ViewDefinitionValidationUtils.QueryPlanAndResources getExplainPlanAndResources(final String sql) throws Exception
  {
    List<Object[]> results = getResults(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        "EXPLAIN PLAN FOR " + sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    String queryPlan = (String) results.get(0)[0];
    String resourceSetSerialized = (String) results.get(0)[1];
    Set<Resource> resources = jsonMapper.readValue(
        resourceSetSerialized,
        new TypeReference<Set<Resource>>()
        {
        }
    );

    return new ViewDefinitionValidationUtils.QueryPlanAndResources(
        queryPlan,
        resources
    );
  }
}
