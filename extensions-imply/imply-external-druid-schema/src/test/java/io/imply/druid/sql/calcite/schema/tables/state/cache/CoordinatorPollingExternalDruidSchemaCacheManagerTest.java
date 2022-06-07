/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.cache.CoordinatorPollingMapCache;
import io.imply.druid.sql.calcite.schema.tables.entity.TableColumn;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class CoordinatorPollingExternalDruidSchemaCacheManagerTest
{
  private static final Map<String, TableSchema> SCHEMA_MAP = ImmutableMap.of(
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

  private Injector injector;
  private AuthorizerMapper authorizerMapper;
  private ImplyExternalDruidSchemaCommonCacheConfig commonCacheConfig;
  private ObjectMapper objectMapper;
  private DruidLeaderClient druidLeaderClient;

  private CoordinatorPollingExternalDruidSchemaCacheManager target;
  private CoordinatorPollingMapCache<TableSchema> schemaCache;

  @Before
  public void setup() throws IOException, InterruptedException
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    objectMapper = new DefaultObjectMapper(smileFactory, null);
    objectMapper.getFactory().setCodec(objectMapper);
    injector = Mockito.mock(Injector.class);
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    Mockito.when(injector.getInstance(AuthorizerMapper.class)).thenReturn(authorizerMapper);

    commonCacheConfig = Mockito.mock(ImplyExternalDruidSchemaCommonCacheConfig.class);
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(200_000L);
    Mockito.when(commonCacheConfig.getMaxSyncRetries()).thenReturn(1);

    druidLeaderClient = Mockito.mock(DruidLeaderClient.class);
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(null);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);

    schemaCache = Mockito.spy(
        CoordinatorPollingExternalDruidSchemaCacheManager.makeSchemaCache(
            commonCacheConfig,
            objectMapper,
            druidLeaderClient
        )
    );
    target = Mockito.spy(
        new CoordinatorPollingExternalDruidSchemaCacheManager(
          commonCacheConfig,
          schemaCache
      )
    );
  }

  @Test
  public void test_start_doesPollCoordinator()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Request schemaRequest = Mockito.mock(Request.class);
    Mockito.when(schemaRequest.getUrl()).thenReturn(new URL("http://localhost:8081/druid-ext/imply-external-druid-schema/cachedSerializedSchemaMap"));
    Mockito.when(druidLeaderClient.makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedSchemaMap")))).thenReturn(schemaRequest);
    BytesFullResponseHolder schemaResponseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] schemaSerialized = objectMapper.writeValueAsBytes(SCHEMA_MAP);
    Mockito.when(schemaResponseHolder.getContent()).thenReturn(schemaSerialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.argThat(req -> req != null && req.getUrl().getPath().contains("cachedSerializedSchemaMap")), ArgumentMatchers.any())).thenReturn(schemaResponseHolder);

    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertEquals(SCHEMA_MAP, target.getTableSchemas());

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedSchemaMap")));
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.argThat(req -> req.getUrl().getPath().contains("cachedSerializedSchemaMap")), ArgumentMatchers.any());
  }

  @Test
  public void test_start_CacheDirectoryPresent_doesPollCoordinatorAndWriteToDisk()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    Request schemaRequest = Mockito.mock(Request.class);
    Mockito.when(schemaRequest.getUrl()).thenReturn(new URL("http://localhost:8081/druid-ext/imply-external-druid-schema/cachedSerializedSchemaMap"));
    Mockito.when(druidLeaderClient.makeRequest(ArgumentMatchers.any(), ArgumentMatchers.argThat(req -> req != null && req.contains("cachedSerializedSchemaMap")))).thenReturn(schemaRequest);
    BytesFullResponseHolder schemaResponseHolder = Mockito.mock(BytesFullResponseHolder.class);
    byte[] schemaSerialized = objectMapper.writeValueAsBytes(SCHEMA_MAP);
    Mockito.when(schemaResponseHolder.getContent()).thenReturn(schemaSerialized);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.argThat(req -> req != null && req.getUrl().getPath().contains("cachedSerializedSchemaMap")), ArgumentMatchers.any())).thenReturn(schemaResponseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
    File expectedCachedSchemaMapFile = new File("/tmp/cache", "table-schemas.cache");
    Mockito.verify(schemaCache, Mockito.times(2)).writeFileAtomically(expectedCachedSchemaMapFile, schemaSerialized);
  }

  @Test
  public void test_start_CacheDirectoryPresentAndNullSchemasFromCoordinator_doesPollCoordinatorAndDoesNotWriteToDisk()
      throws IOException, InterruptedException
  {
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L, 200_000L);
    Mockito.when(commonCacheConfig.getMaxRandomDelay()).thenReturn(1L);
    Mockito.when(commonCacheConfig.getCacheDirectory()).thenReturn("/tmp/cache");
    Mockito.when(commonCacheConfig.isEnableCacheNotifications()).thenReturn(true);
    BytesFullResponseHolder responseHolder = Mockito.mock(BytesFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(null);
    Mockito.when(druidLeaderClient.go(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(responseHolder);
    target.start();
    // sleep to wait for scheduled thread executer to run through an interation.
    Thread.sleep(5000L);

    Assert.assertNull(target.getTableSchemas());

    Mockito.verify(druidLeaderClient, Mockito.times(2)).makeRequest(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(druidLeaderClient, Mockito.times(2)).go(ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verify(schemaCache, Mockito.never()).writeFileAtomically(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test (expected = ISE.class)
  public void test_start_twice_throwsException()
  {
    target.start();
    target.start();
  }

  @Test (expected = IllegalMonitorStateException.class)
  public void test_stop_beforeStart_throwsException()
  {
    target.stop();
  }

  @Test
  public void test_stop_afterStart_succeeds()
  {
    target.start();
    target.stop();
  }

  @Test (expected = ISE.class)
  public void test_stop_twice_throwsException()
  {
    target.start();
    target.stop();
    target.stop();
  }
}
