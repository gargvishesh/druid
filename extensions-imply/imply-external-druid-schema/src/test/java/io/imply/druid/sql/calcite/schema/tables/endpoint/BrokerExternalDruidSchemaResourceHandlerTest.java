/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.tables.entity.TableColumn;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.state.cache.CoordinatorPollingExternalDruidSchemaCacheManager;
import io.imply.druid.sql.calcite.schema.tables.state.cache.ExternalDruidSchemaCacheManager;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.util.Map;

public class BrokerExternalDruidSchemaResourceHandlerTest
{
  private ExternalDruidSchemaCacheManager cacheManager;
  private ObjectMapper objectMapper = new ObjectMapper();
  private BrokerExternalDruidSchemaResourceHandler target;

  @Before
  public void setup()
  {
    DruidLeaderClient druidLeaderClient = Mockito.mock(DruidLeaderClient.class);
    cacheManager = new CoordinatorPollingExternalDruidSchemaCacheManager(
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
        objectMapper,
        druidLeaderClient
    );

    target = new BrokerExternalDruidSchemaResourceHandler(cacheManager);
  }

  @Test
  public void test_tableSchemaUpdateListener_succeeds() throws JsonProcessingException
  {
    Map<String, TableSchema> schemaMap = ImmutableMap.of(
        "table1", new TableSchema(
            "table1",
            ImmutableList.of(
                new TableColumn("col1", ColumnType.STRING),
                new TableColumn("col2", ColumnType.LONG)
            )
        ),
        "table2", new TableSchema(
            "table2",
            ImmutableList.of(
                new TableColumn("col1", ColumnType.STRING),
                new TableColumn("col2", ColumnType.FLOAT)
            )
        )
    );
    byte[] serializedMap = objectMapper.writeValueAsBytes(schemaMap);
    target.tableSchemaUpdateListener(serializedMap);
    Assert.assertEquals(schemaMap, cacheManager.getTableSchemas());
  }

  @Test
  public void test_getCachedTableSchemaMaps_notFound()
  {
    Response response = target.getCachedTableSchemaMaps();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
  }
}
