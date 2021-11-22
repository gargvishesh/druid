/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.schema.tables.entity.TableColumn;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.cache.CoordinatorPollingExternalDruidSchemaCacheManager;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class ImplyDruidSchemaManagerTest extends BaseCalciteQueryTest
{
  private CoordinatorPollingExternalDruidSchemaCacheManager schemaCacheManager;
  private ImplyDruidSchemaManager schemaManager;
  private PlannerFactory plannerFactory;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp2()
  {
    NullHandling.sqlCompatible();
    this.schemaCacheManager =
        new CoordinatorPollingExternalDruidSchemaCacheManager(
            new ImplyExternalDruidSchemaCommonCacheConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "http://test:9000"
            ),
            jsonMapper,
            Mockito.mock(DruidLeaderClient.class)
        );
    this.schemaManager = new ImplyDruidSchemaManager(
        schemaCacheManager
    );

    DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        conglomerate,
        walker,
        PLANNER_CONFIG_DEFAULT,
        new NoopViewManager(),
        schemaManager,
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
  public void testSchemaManager() throws Exception
  {
    Map<String, TableSchema> schemaMap = ImmutableMap.of(
        CalciteTests.DATASOURCE1,
        new TableSchema(
            CalciteTests.DATASOURCE1,
            ImmutableList.of(
                new TableColumn("__time", ColumnType.LONG),
                new TableColumn("m1", ColumnType.DOUBLE),
                new TableColumn("m2", ColumnType.DOUBLE),
                new TableColumn("dim1", ColumnType.STRING),
                new TableColumn("dim2", ColumnType.STRING),
                new TableColumn("dim3", ColumnType.STRING)
            )
        )
    );
    schemaCacheManager.updateTableSchemas(
        jsonMapper.writeValueAsBytes(schemaMap)
    );

    ScanQuery query = Druids.newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .context(QUERY_CONTEXT_DEFAULT)
                            .columns("__time", "dim1", "dim2", "dim3", "m1", "m2")
                            .legacy(false)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .build();

    List<Object[]> expectedRows;
    if (NullHandling.sqlCompatible()) {
      expectedRows = ImmutableList.of(
          new Object[]{946684800000L, 1.0d, 1.0d, "", "a", "[\"a\",\"b\"]"},
          new Object[]{946771200000L, 2.0d, 2.0d, "10.1", null, "[\"b\",\"c\"]"},
          new Object[]{946857600000L, 3.0d, 3.0d, "2", "", "d"},
          new Object[]{978307200000L, 4.0d, 4.0d, "1", "a", ""},
          new Object[]{978393600000L, 5.0d, 5.0d, "def", "abc", null},
          new Object[]{978480000000L, 6.0d, 6.0d, "abc", null, null}
      );
    } else {
      expectedRows = ImmutableList.of(
          new Object[]{946684800000L, 1.0d, 1.0d, "", "a", "[\"a\",\"b\"]"},
          new Object[]{946771200000L, 2.0d, 2.0d, "10.1", "", "[\"b\",\"c\"]"},
          new Object[]{946857600000L, 3.0d, 3.0d, "2", "", "d"},
          new Object[]{978307200000L, 4.0d, 4.0d, "1", "a", ""},
          new Object[]{978393600000L, 5.0d, 5.0d, "def", "abc", ""},
          new Object[]{978480000000L, 6.0d, 6.0d, "abc", "", ""}
      );
    }

    testQuery(
        "SELECT * FROM druid.foo",
        ImmutableList.of(
          query
        ),
        expectedRows
    );

    // remove some columns, add non-existent column, and change type of m1,
    Map<String, TableSchema> schemaMap2 = ImmutableMap.of(
        CalciteTests.DATASOURCE1,
        new TableSchema(
            CalciteTests.DATASOURCE1,
            ImmutableList.of(
                new TableColumn("__time", ColumnType.LONG),
                new TableColumn("m1", ColumnType.FLOAT),
                new TableColumn("dim1", ColumnType.STRING),
                new TableColumn("dim3", ColumnType.STRING),
                new TableColumn("dim99", ColumnType.STRING)
            )
        )
    );
    schemaCacheManager.updateTableSchemas(
        jsonMapper.writeValueAsBytes(schemaMap2)
    );

    ScanQuery query2 = Druids.newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .context(QUERY_CONTEXT_DEFAULT)
                            .columns("__time", "dim1", "dim3", "dim99", "m1")
                            .legacy(false)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .build();

    if (NullHandling.sqlCompatible()) {
      expectedRows = ImmutableList.of(
          new Object[]{946684800000L, 1.0f, "", "[\"a\",\"b\"]", null},
          new Object[]{946771200000L, 2.0f, "10.1", "[\"b\",\"c\"]", null},
          new Object[]{946857600000L, 3.0f, "2", "d", null},
          new Object[]{978307200000L, 4.0f, "1", "", null},
          new Object[]{978393600000L, 5.0f, "def", null, null},
          new Object[]{978480000000L, 6.0f, "abc", null, null}
      );
    } else {
      expectedRows = ImmutableList.of(
          new Object[]{946684800000L, 1.0f, "", "[\"a\",\"b\"]", ""},
          new Object[]{946771200000L, 2.0f, "10.1", "[\"b\",\"c\"]", ""},
          new Object[]{946857600000L, 3.0f, "2", "d", ""},
          new Object[]{978307200000L, 4.0f, "1", "", ""},
          new Object[]{978393600000L, 5.0f, "def", "", ""},
          new Object[]{978480000000L, 6.0f, "abc", "", ""}
      );
    }

    testQuery(
        "SELECT * FROM druid.foo",
        ImmutableList.of(
            query2
        ),
        expectedRows
    );
  }
}
