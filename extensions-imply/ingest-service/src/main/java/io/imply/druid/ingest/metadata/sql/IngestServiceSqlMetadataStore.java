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
import io.imply.druid.ingest.metadata.Table;
import io.imply.druid.ingest.metadata.TableJobStateStats;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    return metadataConnector.getDBI().inTransaction(
        (handle, transactionStatus) ->
            handle.createStatement(
                StringUtils.format(
                    "UPDATE %1$s SET scheduled_timestamp=:ts, schema_blob=:sc, job_state=:st WHERE job_id=:jid",
                    ingestConfig.get().getJobsTable()
                )
            )
                  .bind("ts", scheduled)
                  .bind("sc", jsonMapper.writeValueAsBytes(schema))
                  .bind("st", JobState.SCHEDULED)
                  .bind("jid", jobId)
                  .execute()
    );
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
  public void setJobStateAndStatus(String jobId, @Nullable JobStatus status, @Nullable JobState jobState)
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
    setJobStateAndStatus(jobId, status, JobState.CANCELLED);
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
  public Set<TableJobStateStats> getJobCountPerTablePerState(@Nullable Set<JobState> statesToFilterOn)
  {
    final String baseQueryStr = "SELECT t.name as tn,  count(j.job_state) as cnt, j.job_state as js, "
                                + "t.created_timestamp as tcs FROM ingest_tables t "
                                + "LEFT JOIN ingest_jobs j ON j.table_name  = t.name "
                                + "GROUP BY t.name, t.created_timestamp, j.job_state "
                                + "%s ";

    List<TableJobStateStats> tableJobStateStats =
        metadataConnector.getDBI().inTransaction(
            (handle, status) -> {
              String queryStr;
              if (statesToFilterOn == null) {
                // all tables
                queryStr = String.format(baseQueryStr, "ORDER BY t.name");
              } else if (statesToFilterOn.size() == 0) {
                // only tables with no jobs
                queryStr = String.format(baseQueryStr, "HAVING cnt = 0 ORDER BY t.name");
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

                String tail = String.format("HAVING j.job_state IN %s ORDER by t.name", inList);
                queryStr = String.format(baseQueryStr, tail);
              }
              final Query<Map<String, Object>> query = handle.createQuery(queryStr);
              // get tables with their corresponding states
              Map<String, TableJobStateStats> tableJobsMap = new HashMap<>();
              return query.map(
                  (index, r, ctx) -> {
                    // get sql results:
                    String tn = r.getString("tn");
                    int count = r.getInt("cnt");
                    JobState js = Optional.ofNullable(r.getString("js")).map(jss -> JobState.fromString(jss))
                                          .orElse(null);
                    DateTime tct = DateTimes.of(r.getString("tcs"));
                    // update table map cache:
                    if (tableJobsMap.computeIfPresent(tn, (k, v) -> v.addJobState(js, count)) == null) {
                      tableJobsMap.put(tn, new TableJobStateStats(tn, tct, js, count));
                    }
                    return tableJobsMap.get(tn);
                  }).list();
            }
        );

    return new HashSet<>(tableJobStateStats);
  }

  @Nonnull
  private IngestJob resultRowAsIngestJob(ResultSet r) throws SQLException
  {
    try {
      IngestJob job = new IngestJob(
          r.getString("table_name"),
          r.getString("job_id"),
          JobState.valueOf(r.getString("job_state"))
      )
          .setRetryCount(r.getInt("retry_count"))
          .setCreatedTime(DateTimes.utc((r.getLong("created_timestamp"))));

      Long scheduledTime = (Long) r.getObject("scheduled_timestamp");
      if (scheduledTime != null) {
        job.setScheduledTime(DateTimes.utc(scheduledTime));
      }

      Long schemaId = (Long) r.getObject("schema_id");
      if (schemaId != null) {
        job.setSchemaId(schemaId);
      }

      byte[] ingestSchemaBlob = r.getBytes("schema_blob");
      if (ingestSchemaBlob != null) {
        job.setSchema(jsonMapper.readValue(ingestSchemaBlob, IngestSchema.class));
      }

      byte[] jobStatusBlob = r.getBytes("job_status");
      if (jobStatusBlob != null) {
        job.setJobStatus(jsonMapper.readValue(jobStatusBlob, JobStatus.class));
      }

      byte[] jobType = r.getBytes("job_type");
      if (jobType != null) {
        job.setJobRunner(jsonMapper.readValue(jobType, JobRunner.class));
      }
      return job;
    }
    catch (IOException e) {
      // todo: something?
      throw new RuntimeException();
    }
  }

  private String getIngestJobBaseQuery()
  {
    return StringUtils.format(
        "SELECT created_timestamp, table_name, job_id, job_state, job_type, schema_blob, schema_id, scheduled_timestamp, job_status, retry_count  FROM %1$s",
        ingestConfig.get().getJobsTable()
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
        "SELECT created_timestamp, schema_blob FROM %1$s",
        ingestConfig.get().getSchemasTable()
    );
  }

  @Nonnull
  private IngestSchema resultRowAsIngestSchema(ResultSet r) throws SQLException
  {
    try {
      byte[] ingestSchemaBlob = r.getBytes("schema_blob");
      if (ingestSchemaBlob != null) {
        return jsonMapper.readValue(ingestSchemaBlob, IngestSchema.class);
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
  public IngestSchema getSchema(int schemaId)
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) ->
            handle.createQuery(StringUtils.format("%s WHERE id=:scid", getSchemaBaseQuery()))
                  .bind("scid", schemaId)
                  .map((index, r, ctx) -> resultRowAsIngestSchema(r))
                  .first()
    );
  }

  @Override
  public List<IngestSchema> getAllSchemas()
  {
    return metadataConnector.getDBI().inTransaction(
        (handle, status) -> {
          final Query<Map<String, Object>> query = handle.createQuery(getSchemaBaseQuery());
          return query.map((index, r, ctx) -> resultRowAsIngestSchema(r))
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
