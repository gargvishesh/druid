/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.JobStatus;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.metadata.JobScheduleException;
import io.imply.druid.ingest.metadata.StoredIngestSchema;
import io.imply.druid.ingest.metadata.Table;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Folder2;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * fill me out
 */
public class IngestServiceSqlMetadataStore implements IngestServiceMetadataStore
{
  private final Supplier<IngestServiceSqlMetatadataConfig> ingestConfig;
  private final SQLMetadataConnector metadataConnector;
  private final ObjectMapper jsonMapper;

  @Inject
  public IngestServiceSqlMetadataStore(
      Supplier<IngestServiceSqlMetatadataConfig> ingestConfig,
      SQLMetadataConnector metadataConnector,
      @Json ObjectMapper jsonMapper
  )
  {
    this.ingestConfig = ingestConfig;
    this.metadataConnector = metadataConnector;
    this.jsonMapper = jsonMapper;
    if (ingestConfig.get().shouldCreateTables()) {
      createTablesTable();
      createJobsTable();
      createSchemasTable();
    }
  }

  public void createJobsTable()
  {
    // schema WIP, probably missing stuffs
    final String tableName = ingestConfig.get().getJobsTable();
    metadataConnector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  created_timestamp BIGINT NOT NULL,\n"
                + "  table_name VARCHAR(255) NOT NULL,\n"
                + "  job_id VARCHAR(255),\n"
                + "  job_type %3$s,\n"
                + "  job_state VARCHAR(255) NOT NULL,\n"
                + "  job_status %3$s,\n"
                + "  schema_blob %3$s,\n"
                + "  schema_id BIGINT,\n"
                + "  scheduled_timestamp BIGINT,\n"
                + "  retry_count INT,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName,
                metadataConnector.getSerialType(),
                metadataConnector.getPayloadType()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_table_name ON %1$s(table_name)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_created_timestamp ON %1$s(created_timestamp DESC)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_scheduled_timestamp ON %1$s(scheduled_timestamp)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_job_state ON %1$s(job_state)", tableName)
        )
    );
  }

  @Override
  public String stageJob(String tableName, JobRunner jobType)
  {
    final String jobId = UUIDUtils.generateUuid();
    final long createdTimestamp = DateTimes.nowUtc().getMillis();
    metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          handle.createStatement(
              StringUtils.format(
                  "INSERT INTO %1$s (created_timestamp, table_name, job_id, job_type, job_state) VALUES (:ts, :tn, :jid, :jt, :js)",
                  ingestConfig.get().getJobsTable()
              )
          )
                .bind("ts", createdTimestamp)
                .bind("tn", tableName)
                .bind("jid", jobId)
                .bind("jt", jsonMapper.writeValueAsBytes(jobType))
                .bind("js", JobState.STAGED)
                .execute();
          return null;
        }
    );
    return jobId;
  }

  @Override
  public int scheduleJob(String jobId, IngestSchema schema)
  {
    final long scheduled = DateTimes.nowUtc().getMillis();
    final AtomicReference<JobState> invalidState = new AtomicReference<>();
    final int updated = metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          final String selectQuery = StringUtils.format(
              "SELECT job_state FROM %1$s WHERE job_id=:jid",
              ingestConfig.get().getJobsTable()
          );

          JobState currentState = handle.createQuery(selectQuery)
                                        .bind("jid", jobId)
                                        .map((index, r, ctx) -> JobState.valueOf(r.getString("job_state")))
                                        .first();

          if (currentState != null && !currentState.canSchedule()) {
            invalidState.set(currentState);
            return 0;
          }
          final String updateQuery = StringUtils.format(
              "UPDATE %1$s SET scheduled_timestamp=:ts, schema_blob=:sc, schema_id=null, job_state=:st, job_status=null WHERE job_id=:jid",
              ingestConfig.get().getJobsTable()
          );
          return handle.createStatement(updateQuery)
                       .bind("ts", scheduled)
                       .bind("sc", jsonMapper.writeValueAsBytes(schema))
                       .bind("st", JobState.SCHEDULED)
                       .bind("jid", jobId)
                       .execute();
        }
    );
    if (updated == 0 && invalidState.get() != null) {
      throw new JobScheduleException(jobId, invalidState.get());
    }
    return updated;
  }

  @Override
  public int scheduleJob(String jobId, int schemaId) throws JobScheduleException
  {
    final long scheduled = DateTimes.nowUtc().getMillis();
    final AtomicReference<JobState> invalidState = new AtomicReference<>();
    final int updated = metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          final String selectQuery = StringUtils.format(
              "SELECT job_state FROM %1$s WHERE job_id=:jid",
              ingestConfig.get().getJobsTable()
          );

          JobState currentState = handle.createQuery(selectQuery)
                                        .bind("jid", jobId)
                                        .map((index, r, ctx) -> JobState.valueOf(r.getString("job_state")))
                                        .first();

          if (currentState != null && !currentState.canSchedule()) {
            invalidState.set(currentState);
            return 0;
          }
          final String updateQuery = StringUtils.format(
              "UPDATE %1$s SET scheduled_timestamp=:ts, schema_blob=null, schema_id=:sc, job_state=:st, job_status=null WHERE job_id=:jid",
              ingestConfig.get().getJobsTable()
          );
          return handle.createStatement(updateQuery)
                       .bind("ts", scheduled)
                       .bind("sc", schemaId)
                       .bind("st", JobState.SCHEDULED)
                       .bind("jid", jobId)
                       .execute();
        }
    );
    if (updated == 0 && invalidState.get() != null) {
      throw new JobScheduleException(jobId, invalidState.get());
    }
    return updated;
  }

  @Override
  public void setJobStatus(String jobId, JobStatus jobStatus)
  {
    metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          final byte[] blob = jsonMapper.writeValueAsBytes(jobStatus);
          handle.createStatement(
              StringUtils.format(
                  "UPDATE %1$s SET job_status=:js WHERE job_id=:jid",
                  ingestConfig.get().getJobsTable()
              )
          )
                .bind("js", blob)
                .bind("jid", jobId)
                .execute();
          return null;
        }
    );
  }

  @Override
  public void setJobState(String jobId, JobState jobState)
  {
    metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          handle.createStatement(
              StringUtils.format(
                  "UPDATE %1$s SET job_state=:st WHERE job_id=:jid",
                  ingestConfig.get().getJobsTable()
              )
          )
                .bind("st", jobState)
                .bind("jid", jobId)
                .execute();
          return null;
        }
    );
  }

  @Override
  public void setJobStateAndStatus(
      String jobId,
      @Nullable JobState jobState,
      @Nullable JobStatus status
  )
  {
    metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          final byte[] blob = status != null ? jsonMapper.writeValueAsBytes(status) : null;
          handle.createStatement(
              StringUtils.format(
                  "UPDATE %1$s SET job_state=:st, job_status=:js WHERE job_id=:jid",
                  ingestConfig.get().getJobsTable()
              )
          )
                .bind("st", jobState)
                .bind("js", blob)
                .bind("jid", jobId)
                .execute();
          return null;
        }
    );
  }

  @Override
  public void setJobCancelled(String jobId, JobStatus status)
  {
    setJobStateAndStatus(jobId, JobState.CANCELLED, status);
  }

  @Override
  public int jobRetry(String jobId)
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          String query = StringUtils.format(
              "SELECT retry_count FROM %1$s WHERE job_id=:jid ORDER BY created_timestamp DESC",
              ingestConfig.get().getJobsTable()
          );

          Integer retryCount = handle
              .createQuery(query)
              .bind("jid", jobId)
              .map(IntegerMapper.FIRST)
              .first();

          if (retryCount != null) {
            final int nextCount = retryCount + 1;
            handle.createStatement(
                StringUtils.format(
                    "UPDATE %1$s SET retry_count=:rt WHERE job_id=:jid",
                    ingestConfig.get().getJobsTable()
                )
            )
                  .bind("rt", nextCount)
                  .bind("jid", jobId)
                  .execute();

            return nextCount;
          }
          return 0;
        }
    );
  }

  @Override
  public @Nullable
  IngestJob getJob(String jobId)
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) ->
            handle.createQuery(StringUtils.format("%s WHERE job_id=:jid", getIngestJobBaseQuery()))
                  .bind("jid", jobId)
                  .map((index, r, ctx) -> resultRowAsIngestJob(r))
                  .first()
    );
  }

  @Nullable
  @Override
  public String getJobTable(String jobId)
  {
    final String query = StringUtils.format(
        "SELECT table_name FROM %1$s WHERE job_id=:jid",
        ingestConfig.get().getJobsTable()
    );
    return metadataConnector.getDBI().inTransaction(
        (handle, status) ->
            handle.createQuery(query)
                  .bind("jid", jobId)
                  .map((index, r, ctx) -> r.getString("table_name"))
                  .first()
    );
  }

  @Override
  public List<IngestJob> getJobs(@Nullable JobState jobState)
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) -> {
          final Query<Map<String, Object>> query;

          String baseQuery = getIngestJobBaseQuery();

          if (jobState != null) {
            if (JobState.SCHEDULED.equals(jobState)) {
              baseQuery = StringUtils.format("%s WHERE job_state=:js ORDER BY scheduled_timestamp", baseQuery);
            } else {
              baseQuery = StringUtils.format("%s WHERE job_state=:js", baseQuery);
            }
            query = handle.createQuery(baseQuery).bind("js", jobState.name());
          } else {
            query = handle.createQuery(baseQuery);
          }

          return query.map((index, r, ctx) -> resultRowAsIngestJob(r))
                      .list();
        });
  }

  @Override
  public Map<Table, Object2IntMap<JobState>> getTableJobSummary(@Nullable Set<JobState> statesToFilterOn)
  {
    final String baseQueryStr = "SELECT"
                                + " t.name as tn,"
                                + " count(j.job_state) as cnt,"
                                + " j.job_state as js,"
                                + " t.created_timestamp as tcs"
                                + " FROM ingest_tables t"
                                + " LEFT JOIN ingest_jobs j ON j.table_name = t.name"
                                + " GROUP BY t.name, t.created_timestamp, j.job_state"
                                + " %s ";

    return metadataConnector.getDBI().inTransaction(
        (handle, status) -> {
          String queryStr;
          if (statesToFilterOn == null) {
            // all tables
            queryStr = StringUtils.format(baseQueryStr, "ORDER BY t.name");
          } else if (statesToFilterOn.size() == 0) {
            // only tables with no jobs
            queryStr = StringUtils.format(baseQueryStr, "HAVING cnt = 0 ORDER BY t.name");
          } else {
            // Add states constraint HAVING clause....
            // build IN list:
            StringBuilder inList = new StringBuilder("(");
            boolean first = true;
            for (JobState st : statesToFilterOn) {
              if (!first) {
                inList.append(",");
              } else {
                first = false;
              }
              inList.append("'").append(st.name()).append("'");
            }
            inList.append(")");

            String tail = StringUtils.format("HAVING j.job_state IN %s ORDER by t.name", inList);
            queryStr = StringUtils.format(baseQueryStr, tail);
          }
          return handle.createQuery(queryStr)
                       .fold(new HashMap<>(), (Folder2<Map<Table, Object2IntMap<JobState>>>) (accumulator, rs, ctx) -> {
                         // get sql results:
                         final String tableName = rs.getString("tn");
                         final DateTime createdTime = DateTimes.of(rs.getString("tcs"));
                         final JobState jobState = Optional.ofNullable(rs.getString("js"))
                                                     .map(JobState::fromString)
                                                     .orElse(null);
                         final int jobStateCount = rs.getInt("cnt");
                         final Table t = new Table(tableName, createdTime);
                         if (jobState != null) {
                           accumulator.compute(t, (table, map) -> {
                             if (map == null) {
                               map = new Object2IntOpenHashMap<>();
                             }
                             // since this is part of group by, individual job states will already be fully
                             // accumulated, so can just use count directly
                             map.put(jobState, jobStateCount);
                             return map;
                           });
                         }
                         // if no jobs in any state, still include the table
                         accumulator.putIfAbsent(t, null);

                         return accumulator;
                       });
        });
  }

  @Nonnull
  private IngestJob resultRowAsIngestJob(ResultSet r) throws SQLException
  {
    try {
      byte[] jobStatusBlob = r.getBytes("job_status");
      JobStatus jobStatus = null;
      if (jobStatusBlob != null) {
        jobStatus = jsonMapper.readValue(jobStatusBlob, JobStatus.class);
      }

      byte[] jobType = r.getBytes("job_type");
      JobRunner jobRunner = null;
      if (jobType != null) {
        jobRunner = jsonMapper.readValue(jobType, JobRunner.class);
      }

      byte[] ingestSchemaBlob = r.getBytes("schema_blob");
      byte[] externalSchemaBlob = r.getBytes("external_schema_blob");
      IngestSchema schema = null;
      if (ingestSchemaBlob != null) {
        schema = jsonMapper.readValue(ingestSchemaBlob, IngestSchema.class);
      } else if (externalSchemaBlob != null) {
        schema = jsonMapper.readValue(externalSchemaBlob, IngestSchema.class);
      }
      Long scheduledTimeMillis = (Long) r.getObject("scheduled_timestamp");
      DateTime scheduledTime = null;
      if (scheduledTimeMillis != null) {
        scheduledTime = DateTimes.utc(scheduledTimeMillis);
      }

      return new IngestJob(
          r.getString("table_name"),
          r.getString("job_id"),
          JobState.valueOf(r.getString("job_state")),
          jobStatus,
          jobRunner,
          schema,
          DateTimes.utc((r.getLong("created_timestamp"))),
          scheduledTime,
          r.getInt("retry_count")
      );
    }
    catch (IOException e) {
      // todo: something?
      throw new RuntimeException();
    }
  }

  private String getIngestJobBaseQuery()
  {
    return StringUtils.format(
        "SELECT"
        + " j.created_timestamp as created_timestamp,"
        + " table_name,"
        + " job_id,"
        + " job_state,"
        + " job_type,"
        + " j.schema_blob as schema_blob,"
        + " s.schema_blob as external_schema_blob,"
        + " scheduled_timestamp,"
        + " job_status,"
        + " retry_count "
        + " FROM %1$s j"
        + " LEFT JOIN %2$s s ON j.schema_id = s.id",
        ingestConfig.get().getJobsTable(),
        ingestConfig.get().getSchemasTable()
    );
  }

  public void createSchemasTable()
  {
    final String tableName = ingestConfig.get().getSchemasTable();
    metadataConnector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  created_timestamp BIGINT NOT NULL,\n"
                + "  schema_blob %3$s,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName,
                metadataConnector.getSerialType(),
                metadataConnector.getPayloadType()
            )
        )
    );
  }

  @Override
  public int createSchema(IngestSchema ingestSchema)
  {
    long timeStamp = DateTimes.nowUtc().getMillis();
    return metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) ->
            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (created_timestamp, schema_blob) VALUES (:ts, :schemablob)",
                    ingestConfig.get().getSchemasTable()
                )
            )
                  .bind("ts", timeStamp)
                  .bind("schemablob", jsonMapper.writeValueAsBytes(ingestSchema))
                  .executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first()
    );
  }

  private String getSchemaBaseQuery()
  {
    return StringUtils.format(
        "SELECT id, created_timestamp, schema_blob FROM %1$s",
        ingestConfig.get().getSchemasTable()
    );
  }

  @Nonnull
  private StoredIngestSchema resultRowAsIngestSchema(int schemaId, ResultSet r) throws SQLException
  {
    try {
      byte[] ingestSchemaBlob = r.getBytes("schema_blob");
      if (ingestSchemaBlob != null) {
        return new StoredIngestSchema(schemaId, jsonMapper.readValue(ingestSchemaBlob, IngestSchema.class));
      } else {
        throw new RuntimeException("schema blob was null!");
      }
    }
    catch (IOException e) {
      // todo: something?
      throw new RuntimeException();
    }
  }

  @Override
  public StoredIngestSchema getSchema(int schemaId)
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) ->
            handle.createQuery(StringUtils.format("%s WHERE id=:scid", getSchemaBaseQuery()))
                  .bind("scid", schemaId)
                  .map((index, r, ctx) -> resultRowAsIngestSchema(schemaId, r))
                  .first()
    );
  }

  @Override
  public boolean schemaExists(int schemaId)
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) ->
            handle.createQuery(StringUtils.format("SELECT COUNT(1) as count FROM %1$s WHERE id=:scid", ingestConfig.get().getSchemasTable()))
                  .bind("scid", schemaId)
                  .map((index, r, ctx) -> r.getInt("count"))
                  .first()
    ) > 0;
  }

  @Override
  public List<StoredIngestSchema> getAllSchemas()
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) -> {
          final Query<Map<String, Object>> query = handle.createQuery(getSchemaBaseQuery());
          return query.map((index, r, ctx) -> resultRowAsIngestSchema(r.getInt("id"), r))
                      .list();
        });
  }

  @Override
  public int deleteSchema(int schemaId)
  {
    return metadataConnector.getDBI().inTransaction(
        new TransactionCallback<Integer>()
        {
          @Override
          public Integer inTransaction(Handle handle, TransactionStatus transactionStatus)
          {
            return handle.createStatement(StringUtils.format(
                "DELETE from %1$s WHERE id = :id",
                ingestConfig.get().getSchemasTable()
            ))
                         .bind("id", schemaId)
                         .execute();
          }
        }
    );
  }

  @Override
  public InputFormat getFormat(int formatId)
  {
    return null;
  }

  // tables
  public void createTablesTable()
  {
    // schema WIP, probably missing stuffs
    final String tableName = ingestConfig.get().getTablesTable();
    metadataConnector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  created_timestamp BIGINT NOT NULL,\n"
                + "  PRIMARY KEY (name)\n"
                + ")",
                tableName
            )
        )
    );
  }

  @Override
  public int insertTable(String name)
  {
    long timeStamp = DateTimes.nowUtc().getMillis();
    return metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) ->
            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (name, created_timestamp) VALUES (:n, :ts)",
                    ingestConfig.get().getTablesTable()
                )
            )
                  .bind("n", name)
                  .bind("ts", timeStamp)
                  .execute()
    );
  }

  @Override
  public boolean druidTableExists(String name)
  {
    final String query = StringUtils.format(
        "SELECT name FROM %s WHERE name = :name",
        ingestConfig.get().getTablesTable()
    );
    String existingName = metadataConnector.getDBI().withHandle(
        handle -> handle.createQuery(query)
                        .bind("name", name)
                        .map((index, r, ctx) -> r.getString("name"))
                        .first()
    );
    return existingName != null;
  }

  @Override
  public List<Table> getTables()
  {
    final String query = StringUtils.format(
        "SELECT name, created_timestamp FROM %s",
        ingestConfig.get().getTablesTable()
    );
    List<Table> tables = metadataConnector.getDBI().withHandle(
        handle -> handle.createQuery(query)
                        .map((index, r, ctx) ->
                                 new Table(
                                     r.getString("name"),
                                     DateTimes.of(r.getString("created_timestamp"))
                                 )
                        ).list()
    );

    return tables;
  }

}
