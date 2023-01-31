/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.schema.tables.entity.TableColumn;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.cache.CoordinatorPollingExternalDruidSchemaCacheManager;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerFixture;
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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Override
  public DruidSchemaManager createSchemaManager()
  {
    this.schemaCacheManager =
        new CoordinatorPollingExternalDruidSchemaCacheManager(
            new ImplyExternalDruidSchemaCommonConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "http://test:9000/v2/tableSchemas"
            ),
            queryFramework().queryJsonMapper(),
            Mockito.mock(DruidLeaderClient.class)
        );
    this.schemaManager = new ImplyDruidSchemaManager(
        schemaCacheManager
    );
    return this.schemaManager;
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
  public void testSchemaManager() throws Exception
  {
    // Create this so that the schema manager is created and attached
    // to the planner used for tests.
    PlannerFixture plannerFixture = plannerFixture();
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
        queryFramework().queryJsonMapper().writeValueAsBytes(schemaMap)
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

    // Use the test builder directly to reuse the existing planner.
    testBuilder()
      .plannerFixture(plannerFixture)
      .sql("SELECT * FROM druid.foo")
      .expectedQuery(query)
      .expectedResults(expectedRows)
      .run();

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
        queryFramework().queryJsonMapper().writeValueAsBytes(schemaMap2)
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

    testBuilder()
      .plannerFixture(plannerFixture)
      .sql("SELECT * FROM druid.foo")
      .expectedQuery(query2)
      .expectedResults(expectedRows)
      .run();
  }
}
