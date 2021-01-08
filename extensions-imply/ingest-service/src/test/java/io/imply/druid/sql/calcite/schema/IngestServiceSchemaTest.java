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
import com.google.common.collect.ImmutableSet;
import io.imply.druid.ingest.client.IngestServiceClient;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.PartitionScheme;
import io.imply.druid.ingest.server.IngestJobInfo;
import io.imply.druid.ingest.server.IngestJobsResponse;
import io.imply.druid.ingest.server.SchemasResponse;
import io.imply.druid.ingest.server.TableInfo;
import io.imply.druid.ingest.server.TablesResponse;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class IngestServiceSchemaTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private static final DataContext TEST_CONTEXT = new DataContext()
  {
    @Override
    public SchemaPlus getRootSchema()
    {
      return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory()
    {
      return null;
    }

    @Override
    public QueryProvider getQueryProvider()
    {
      return null;
    }

    @Override
    public Object get(String name)
    {
      return CalciteTests.SUPER_USER_AUTH_RESULT;
    }
  };

  public static final TableInfo TEST_TABLE = new TableInfo(
      "some_table",
      DateTimes.nowUtc(),
      ImmutableSet.of(Action.READ, Action.WRITE),
      ImmutableList.of(
          new TableInfo.JobStateCount(JobState.STAGED, 5),
          new TableInfo.JobStateCount(JobState.SCHEDULED, 5),
          new TableInfo.JobStateCount(JobState.RUNNING, 10),
          new TableInfo.JobStateCount(JobState.CANCELLED, 5),
          new TableInfo.JobStateCount(JobState.COMPLETE, 10)
      )
  );

  public static final TableInfo OTHER_TEST_TABLE = new TableInfo(
      "other_table",
      DateTimes.nowUtc(),
      ImmutableSet.of(Action.READ, Action.WRITE),
      null
  );

  public static final IngestSchema TEST_SCHEMA = new IngestSchema(
      new TimestampSpec("time", "iso", null),
      new DimensionsSpec(
          ImmutableList.of(
              StringDimensionSchema.create("column1"),
              StringDimensionSchema.create("column2")
          )
      ),
      new PartitionScheme(Granularities.DAY, null),
      new JsonInputFormat(null, null, null),
      "test schema"
  );

  public static final IngestSchema OTHER_TEST_SCHEMA = new IngestSchema(
      new TimestampSpec("timestamp", "iso", null),
      new DimensionsSpec(
          ImmutableList.of(
              StringDimensionSchema.create("column1"),
              StringDimensionSchema.create("column2"),
              new LongDimensionSchema("column3"),
              new DoubleDimensionSchema("column4")
          )
      ),
      new PartitionScheme(
          Granularities.HOUR,
          new HashedPartitionsSpec(5_000_000, null, ImmutableList.of("column2"))
      ),
      new JsonInputFormat(null, null, null),
      "test schema"
  );

  public static final IngestJobInfo TEST_JOB = new IngestJobInfo(
      TEST_TABLE.getName(),
      UUIDUtils.generateUuid(),
      JobState.RUNNING,
      DateTimes.nowUtc().minus(10 * 1000),
      DateTimes.nowUtc(),
      TEST_SCHEMA,
      null
  );

  public static final IngestJobInfo OTHER_TEST_JOB = new IngestJobInfo(
      OTHER_TEST_TABLE.getName(),
      UUIDUtils.generateUuid(),
      JobState.FAILED,
      DateTimes.nowUtc().minus(10 * 1000),
      DateTimes.nowUtc(),
      OTHER_TEST_SCHEMA,
      "failed!"
  );

  boolean replayed = false;
  IngestServiceClient client;
  AuthorizerMapper authorizerMapper;

  IngestServiceSchema schema;

  @Before
  public void setup()
  {
    this.client = EasyMock.createMock(IngestServiceClient.class);
    this.authorizerMapper = EasyMock.createMock(AuthorizerMapper.class);
    this.schema = new IngestServiceSchema(client, authorizerMapper, MAPPER);
  }

  @After
  public void teardown()
  {
    if (replayed) {
      EasyMock.verify(client, authorizerMapper);
    }
  }

  @Test
  public void testMetadata()
  {
    Assert.assertEquals(ImmutableSet.of("jobs", "tables", "schemas"), schema.getTableMap().keySet());
  }

  @Test
  public void testTablesTable() throws IOException
  {
    Authorizer authorizer = EasyMock.createMock(Authorizer.class);
    expectAuth(authorizer, TEST_TABLE.getName(), Action.READ, true);
    expectAuth(authorizer, OTHER_TEST_TABLE.getName(), Action.READ, false);
    EasyMock.expect(authorizerMapper.getAuthorizer(AuthConfig.ALLOW_ALL_NAME)).andReturn(authorizer).once();
    EasyMock.expect(client.getTables())
            .andReturn(
                new TablesResponse(
                    ImmutableList.of(
                        TEST_TABLE,
                        OTHER_TEST_TABLE
                    )
                )
            )
            .once();
    EasyMock.replay(authorizer);
    replayAll();
    IngestServiceSchema.TablesTable tablesTable = new IngestServiceSchema.TablesTable(client, authorizerMapper, MAPPER);

    List<Object[]> objects = tablesTable.scan(TEST_CONTEXT).toList();

    Assert.assertEquals(1, objects.size());
    Assert.assertArrayEquals(new Object[]{TEST_TABLE.getName(), TEST_TABLE.getCreatedTime()}, objects.get(0));
    EasyMock.verify(authorizer);
  }

  @Test
  public void testSchemasTable() throws IOException
  {
    EasyMock.expect(client.getSchemas())
            .andReturn(new SchemasResponse(ImmutableList.of(TEST_SCHEMA, OTHER_TEST_SCHEMA)))
            .once();
    replayAll();

    IngestServiceSchema.SchemasTable schemasTable =
        new IngestServiceSchema.SchemasTable(client, authorizerMapper, MAPPER);
    List<Object[]> objects = schemasTable.scan(TEST_CONTEXT).toList();
    Assert.assertEquals(2, objects.size());

    Assert.assertArrayEquals(
        new Object[]{
            TEST_SCHEMA.getDescription(),
            MAPPER.writeValueAsString(TEST_SCHEMA.getTimestampSpec()),
            MAPPER.writeValueAsString(TEST_SCHEMA.getDimensionsSpec()),
            MAPPER.writeValueAsString(TEST_SCHEMA.getPartitionScheme()),
            MAPPER.writeValueAsString(TEST_SCHEMA.getInputFormat())
        },
        objects.get(0)
    );
    Assert.assertArrayEquals(
        new Object[]{
            OTHER_TEST_SCHEMA.getDescription(),
            MAPPER.writeValueAsString(OTHER_TEST_SCHEMA.getTimestampSpec()),
            MAPPER.writeValueAsString(OTHER_TEST_SCHEMA.getDimensionsSpec()),
            MAPPER.writeValueAsString(OTHER_TEST_SCHEMA.getPartitionScheme()),
            MAPPER.writeValueAsString(OTHER_TEST_SCHEMA.getInputFormat())
        },
        objects.get(1)
    );
  }

  @Test
  public void testJobsTable() throws IOException
  {
    Authorizer authorizer = EasyMock.createMock(Authorizer.class);
    expectAuth(authorizer, TEST_TABLE.getName(), Action.WRITE, true);
    expectAuth(authorizer, OTHER_TEST_TABLE.getName(), Action.WRITE, false);
    EasyMock.expect(authorizerMapper.getAuthorizer(AuthConfig.ALLOW_ALL_NAME)).andReturn(authorizer).once();

    EasyMock.expect(client.getJobs())
            .andReturn(new IngestJobsResponse(ImmutableList.of(TEST_JOB, OTHER_TEST_JOB)))
            .once();

    EasyMock.replay(authorizer);
    replayAll();

    IngestServiceSchema.JobsTable jobsTable = new IngestServiceSchema.JobsTable(client, authorizerMapper, MAPPER);
    List<Object[]> objects = jobsTable.scan(TEST_CONTEXT).toList();

    Assert.assertEquals(1, objects.size());
    Assert.assertArrayEquals(
        new Object[]{
            TEST_JOB.getTableName(),
            TEST_JOB.getJobId(),
            TEST_JOB.getJobState(),
            TEST_JOB.getCreatedTime(),
            TEST_JOB.getScheduledTime(),
            MAPPER.writeValueAsString(TEST_JOB.getSchema()),
            TEST_JOB.getMessage()
        },
        objects.get(0)
    );

    EasyMock.verify(authorizer);
  }

  private void expectAuth(Authorizer authorizer, String table, Action action, boolean allowed)
  {
    EasyMock.expect(
        authorizer.authorize(
            EasyMock.anyObject(),
            EasyMock.eq(new Resource(table, ResourceType.DATASOURCE)),
            EasyMock.eq(action)
        )
    ).andReturn(new Access(allowed)).once();
  }

  private void replayAll()
  {
    replayed = true;
    EasyMock.replay(client, authorizerMapper);
  }
}
