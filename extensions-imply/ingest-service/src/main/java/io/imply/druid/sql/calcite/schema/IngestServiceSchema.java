/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.ingest.client.IngestServiceClient;
import io.imply.druid.ingest.metadata.StoredIngestSchema;
import io.imply.druid.ingest.server.IngestJobInfo;
import io.imply.druid.ingest.server.IngestJobsResponse;
import io.imply.druid.ingest.server.SchemasResponse;
import io.imply.druid.ingest.server.TableInfo;
import io.imply.druid.ingest.server.TablesResponse;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.DefaultEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class IngestServiceSchema extends AbstractSchema
{
  static final RowSignature TABLES_SIGNATURE = RowSignature
      .builder()
      .add("name", ValueType.STRING)
      .add("created_time", ValueType.STRING)
      .build();

  static final RowSignature SCHEMAS_SIGNATURE = RowSignature
      .builder()
      .add("schema_id", ValueType.STRING)
      .add("description", ValueType.STRING)
      .add("timestamp_spec", ValueType.STRING)
      .add("dimensions_spec", ValueType.STRING)
      .add("partition_scheme", ValueType.STRING)
      .add("input_format", ValueType.STRING)
      .build();

  static final RowSignature JOBS_SIGNATURE = RowSignature
      .builder()
      .add("table_name", ValueType.STRING)
      .add("job_id", ValueType.STRING)
      .add("job_state", ValueType.STRING)
      .add("created_time", ValueType.STRING)
      .add("scheduled_time", ValueType.STRING)
      .add("schema", ValueType.STRING)
      .add("message", ValueType.STRING)
      .build();

  private final Map<String, Table> tableMap;

  @Inject
  public IngestServiceSchema(
      IngestServiceClient ingestServiceClient,
      AuthorizerMapper authorizerMapper,
      @Json ObjectMapper jsonMapper
  )
  {
    this.tableMap = ImmutableMap.of(
        "tables", new TablesTable(ingestServiceClient, authorizerMapper, jsonMapper),
        "jobs", new JobsTable(ingestServiceClient, authorizerMapper, jsonMapper),
        "schemas", new SchemasTable(ingestServiceClient, authorizerMapper, jsonMapper)
    );
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  static class TablesTable extends IngestServiceClientTable<TableInfo>
  {
    public TablesTable(
        IngestServiceClient ingestServiceClient,
        AuthorizerMapper authorizerMapper,
        ObjectMapper jsonMapper
    )
    {
      super(TABLES_SIGNATURE, ingestServiceClient, authorizerMapper, jsonMapper);
    }

    @Override
    Iterator<TableInfo> getObjectIterator(DataContext root) throws IOException
    {
      TablesResponse response = ingestServiceClient.getTables();
      Iterator<TableInfo> it = response.getTables().iterator();
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      Function<TableInfo, Iterable<ResourceAction>> raGenerator = table -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(table.getName()));

      final Iterable<TableInfo> authorizedTasks = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return authorizedTasks.iterator();
    }

    @Override
    Function<TableInfo, Object[]> getRowFunction()
    {
      return t -> new Object[]{
          t.getName(),
          t.getCreatedTime()
      };
    }
  }

  static class SchemasTable extends IngestServiceClientTable<StoredIngestSchema>
  {
    public SchemasTable(
        IngestServiceClient ingestServiceClient,
        AuthorizerMapper authorizerMapper,
        ObjectMapper jsonMapper
    )
    {
      super(SCHEMAS_SIGNATURE, ingestServiceClient, authorizerMapper, jsonMapper);
    }

    @Override
    Iterator<StoredIngestSchema> getObjectIterator(DataContext root) throws IOException
    {
      SchemasResponse response = ingestServiceClient.getSchemas();
      // not really any auth on ingest schemas right now here...
      return response.getSchemas().iterator();
    }

    @Override
    Function<StoredIngestSchema, Object[]> getRowFunction()
    {
      return schema -> {
        String timestampBlob = "";
        String dimensionBlob = "";
        String partitionBlob = "";
        String formatBlob = "";

        try {
          timestampBlob = jsonMapper.writeValueAsString(schema.getTimestampSpec());
          dimensionBlob = jsonMapper.writeValueAsString(schema.getDimensionsSpec());
          partitionBlob = jsonMapper.writeValueAsString(schema.getPartitionScheme());
          formatBlob = jsonMapper.writeValueAsString(schema.getInputFormat());
        }
        catch (JsonProcessingException ignored) {
        }

        return new Object[]{
            schema.getSchemaId(),
            schema.getDescription(),
            timestampBlob,
            dimensionBlob,
            partitionBlob,
            formatBlob
        };
      };
    }
  }

  static class JobsTable extends IngestServiceClientTable<IngestJobInfo>
  {
    public JobsTable(
        IngestServiceClient ingestServiceClient,
        AuthorizerMapper authorizerMapper,
        ObjectMapper jsonMapper
    )
    {
      super(JOBS_SIGNATURE, ingestServiceClient, authorizerMapper, jsonMapper);
    }

    @Override
    Iterator<IngestJobInfo> getObjectIterator(DataContext root) throws IOException
    {
      IngestJobsResponse response = ingestServiceClient.getJobs();
      Iterator<IngestJobInfo> it = response.getJobs().iterator();

      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      // filter by write access to see jobs
      Function<IngestJobInfo, Iterable<ResourceAction>> raGenerator = job -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR.apply(job.getTableName()));

      final Iterable<IngestJobInfo> authorizedJobs = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return authorizedJobs.iterator();
    }

    @Override
    Function<IngestJobInfo, Object[]> getRowFunction()
    {
      return job -> {
        String schemaBlob = null;
        try {
          schemaBlob = jsonMapper.writeValueAsString(job.getSchema());
        }
        catch (JsonProcessingException ignored) {
        }
        return new Object[]{
            job.getTableName(),
            job.getJobId(),
            job.getJobState(),
            job.getCreatedTime(),
            job.getScheduledTime(),
            schemaBlob,
            job.getMessage()
        };
      };
    }
  }

  /**
   * roughly based on sys tables implementations, except no fancy streaming and closeable iterators yet
   */
  abstract static class IngestServiceClientTable<T> extends AbstractTable implements ScannableTable
  {
    protected final RowSignature rowSignature;
    protected final IngestServiceClient ingestServiceClient;
    protected final AuthorizerMapper authorizerMapper;
    protected final ObjectMapper jsonMapper;

    public IngestServiceClientTable(
        RowSignature signature,
        IngestServiceClient ingestServiceClient,
        AuthorizerMapper authorizerMapper,
        ObjectMapper jsonMapper
    )
    {
      this.rowSignature = signature;
      this.ingestServiceClient = ingestServiceClient;
      this.authorizerMapper = authorizerMapper;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(rowSignature, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    abstract Iterator<T> getObjectIterator(DataContext root) throws IOException;

    abstract Function<T, Object[]> getRowFunction();

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      try {
        return new ObjectIteratorToObjectArrayEnumerable<>(
            getObjectIterator(root),
            getRowFunction()
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * ... not as cool as sys schema since it isn't using closeable iterators and streaming responses
   * (this would need a custom {@link org.apache.druid.client.JsonParserIterator} that doesn't assume a top level
   * object is an error response)
   */
  static class ObjectIteratorToObjectArrayEnumerable<T> extends DefaultEnumerable<Object[]>
  {
    private final Iterator<T> it;
    private final Function<T, Object[]> rowFn;

    public ObjectIteratorToObjectArrayEnumerable(Iterator<T> it, Function<T, Object[]> rowFn)
    {
      this.it = it;
      this.rowFn = rowFn;
    }

    @Override
    public Iterator<Object[]> iterator()
    {
      return new Iterator<Object[]>()
      {
        @Override
        public boolean hasNext()
        {
          return it.hasNext();
        }

        @Override
        public Object[] next()
        {
          return rowFn.apply(it.next());
        }
      };
    }

    @Override
    public Enumerator<Object[]> enumerator()
    {
      return new Enumerator<Object[]>()
      {
        @Override
        public Object[] current()
        {
          return rowFn.apply(it.next());
        }

        @Override
        public boolean moveNext()
        {
          return it.hasNext();
        }

        @Override
        public void reset()
        {
        }

        @Override
        public void close()
        {
        }
      };
    }
  }
}
