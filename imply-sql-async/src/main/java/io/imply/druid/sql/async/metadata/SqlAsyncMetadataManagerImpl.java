/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import io.imply.druid.sql.async.exception.AsyncQueryAlreadyExistsException;
import io.imply.druid.sql.async.exception.AsyncQueryDoesNotExistException;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails.State;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsAndMetadata;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.RetryTransactionException;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class SqlAsyncMetadataManagerImpl implements SqlAsyncMetadataManager
{
  private static final int MAX_TRIES_ON_TRANSIENT_ERRORS = 10;

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataStorageConnectorConfig> connectorConfigSupplier;
  private final SqlAsyncMetadataStorageTableConfig tableConfig;
  private final SQLMetadataConnector connector;

  @Inject
  public SqlAsyncMetadataManagerImpl(
      @Json ObjectMapper jsonMapper,
      Supplier<MetadataStorageConnectorConfig> connectorConfigSupplier,
      SqlAsyncMetadataStorageTableConfig tableConfig,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.connectorConfigSupplier = connectorConfigSupplier;
    this.tableConfig = tableConfig;
    this.connector = connector;
  }

  @LifecycleStart
  public void initialize()
  {
    if (connectorConfigSupplier.get().isCreateTables()) {
      connector.createTable(
          tableConfig.getSqlAsyncQueriesTable(),
          ImmutableList.of(
              StringUtils.format(
                  "CREATE TABLE %1$s (\n"
                  + "  id VARCHAR(255) NOT NULL,\n"
                  + "  created_date VARCHAR(255) NOT NULL,\n"
                  + "  state_payload %2$s NOT NULL,\n"
                  + "  metadata_payload %2$s NOT NULL,\n"
                  + "  state_payload_sha1 VARCHAR(255) NOT NULL,"
                  + "  PRIMARY KEY (id)\n"
                  + ")",
                  tableConfig.getSqlAsyncQueriesTable(),
                  connector.getPayloadType()
              )
          )
      );
    }
  }

  @Override
  public void addNewQuery(SqlAsyncQueryDetails queryDetails) throws AsyncQueryAlreadyExistsException
  {
    final boolean uniqueId = connector.retryTransaction(
        (handle, status) -> {
          final boolean exists = existsQueryDetailsWithHandle(handle, queryDetails.getAsyncResultId());
          if (exists) {
            return false;
          }

          final DateTime now = DateTimes.nowUtc();
          final String sql = StringUtils.format(
              "INSERT INTO %1$s (id, created_date, state_payload, metadata_payload, state_payload_sha1) "
              + "VALUES (:id, :created_date, :state_payload, :metadata_payload, :state_payload_sha1)",
              tableConfig.getSqlAsyncQueriesTable()
          );
          byte[] serializedQueryDetails = jsonMapper.writeValueAsBytes(queryDetails);
          final int inserted = handle
              .createStatement(sql)
              .bind("id", queryDetails.getAsyncResultId())
              .bind("created_date", now.toString())
              .bind("state_payload", serializedQueryDetails)
              .bind("metadata_payload", jsonMapper.writeValueAsBytes(new SqlAsyncQueryMetadata(now.getMillis())))
              .bind("state_payload_sha1", sha1(serializedQueryDetails))
              .execute();
          assert inserted == 1;

          return true;
        },
        1,
        1
    );
    if (!uniqueId) {
      throw new AsyncQueryAlreadyExistsException(queryDetails.getAsyncResultId());
    }
  }

  @Override
  public boolean updateQueryDetails(SqlAsyncQueryDetails newQueryDetails) throws AsyncQueryDoesNotExistException
  {
    final QueryDetailsUpdateResult result = connector.retryTransaction(
        (handle, status) -> {
          final SqlAsyncQueryDetails actual = getQueryDetailsWithHandle(handle, newQueryDetails.getAsyncResultId());
          if (actual == null) {
            return QueryDetailsUpdateResult.NOT_FOUND;
          }
          if (actual.getState().isFinal()) {
            return QueryDetailsUpdateResult.CANNOT_UPDATE;
          } else {
            if (!updateQueryDetailsWithHandle(handle, actual, newQueryDetails)) {
              throw new RetryTransactionException(
                  "Aborting transaction due to mismatch between current and expected states. "
                  + "Will retry shortly if there is an attempt left"
              );
            } else {
              return QueryDetailsUpdateResult.SUCCESS;
            }
          }
        },
        3,
        MAX_TRIES_ON_TRANSIENT_ERRORS
    );

    switch (result) {
      case NOT_FOUND:
        throw new AsyncQueryDoesNotExistException(newQueryDetails.getAsyncResultId());
      case SUCCESS:
        return true;
      case CANNOT_UPDATE:
        return false;
      default:
        throw new RE("Unknown result type [%s]", result);
    }
  }

  @Override
  public boolean removeQueryDetails(SqlAsyncQueryDetails queryDetails)
  {
    final String sql = StringUtils.format(
        "DELETE FROM %s WHERE id = :id",
        tableConfig.getSqlAsyncQueriesTable()
    );
    return connector.retryWithHandle(
        handle -> {
          final int deleted = handle.createStatement(sql)
                                    .bind("id", queryDetails.getAsyncResultId())
                                    .execute();
          assert deleted < 2;
          return deleted == 1;
        }
    );
  }

  @Override
  public Optional<SqlAsyncQueryDetails> getQueryDetails(String asyncResultId)
  {
    return Optional.ofNullable(connector.retryWithHandle(handle -> getQueryDetailsWithHandle(handle, asyncResultId)));
  }

  @Override
  public Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadata(String asyncResultId)
  {
    return Optional.ofNullable(
        connector.retryWithHandle(handle -> getQueryDetailsAndMetadataWithHandle(handle, asyncResultId))
    );
  }

  @Override
  public Collection<String> getAllAsyncResultIds()
  {
    return connector.retryWithHandle(
        handle -> handle.createQuery(StringUtils.format("SELECT id FROM %s", tableConfig.getSqlAsyncQueriesTable()))
                        .map(StringMapper.FIRST)
                        .list()
    );
  }

  @Override
  public long totalCompleteQueryResultsSize()
  {
    final String sql = StringUtils.format("SELECT state_payload FROM %s", tableConfig.getSqlAsyncQueriesTable());
    return connector.retryWithHandle(handle -> computeResultLengthSumWithHandle(handle, sql));
  }

  @Override
  public long totalCompleteQueryResultsSize(Collection<String> asyncResultIds)
  {
    final StringBuilder sb = new StringBuilder("SELECT state_payload FROM ");
    sb.append(tableConfig.getSqlAsyncQueriesTable());
    sb.append(" WHERE id IN (");
    for (String asyncResultId : asyncResultIds) {
      sb.append("'").append(asyncResultId).append("',");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(")");
    return connector.retryWithHandle(handle -> computeResultLengthSumWithHandle(handle, sb.toString()));
  }

  private boolean existsQueryDetailsWithHandle(Handle handle, String asyncResultId)
  {
    final String sql = StringUtils.format(
        "SELECT id FROM %1$s WHERE id = :id",
        tableConfig.getSqlAsyncQueriesTable()
    );
    final List<String> matched = handle.createQuery(sql)
                                       .bind("id", asyncResultId)
                                       .map(StringMapper.FIRST)
                                       .list();
    assert matched.size() < 2;
    return !matched.isEmpty();
  }

  @Nullable
  private SqlAsyncQueryDetails getQueryDetailsWithHandle(Handle handle, String asyncResultId) throws IOException
  {
    final String sql = StringUtils.format(
        "SELECT state_payload FROM %1$s WHERE id = :id",
        tableConfig.getSqlAsyncQueriesTable()
    );
    final List<byte[]> matched = handle.createQuery(sql)
                                       .bind("id", asyncResultId)
                                       .map(ByteArrayMapper.FIRST)
                                       .list();
    assert matched.size() < 2;
    if (matched.isEmpty()) {
      return null;
    } else {
      return jsonMapper.readValue(matched.get(0), SqlAsyncQueryDetails.class);
    }
  }

  @Nullable
  private SqlAsyncQueryDetailsAndMetadata getQueryDetailsAndMetadataWithHandle(Handle handle, String asyncResultId)
  {
    final String sql = StringUtils.format(
        "SELECT state_payload, metadata_payload FROM %1$s WHERE id = :id",
        tableConfig.getSqlAsyncQueriesTable()
    );
    final List<SqlAsyncQueryDetailsAndMetadata> matched = handle
        .createQuery(sql)
        .bind("id", asyncResultId)
        .map((index, r, ctx) -> {
          try {
            return new SqlAsyncQueryDetailsAndMetadata(
                jsonMapper.readValue(r.getBytes("state_payload"), SqlAsyncQueryDetails.class),
                jsonMapper.readValue(r.getBytes("metadata_payload"), SqlAsyncQueryMetadata.class)
            );
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .list();
    assert matched.size() < 2;
    if (matched.isEmpty()) {
      return null;
    } else {
      return matched.get(0);
    }
  }

  private boolean updateQueryDetailsWithHandle(
      Handle handle,
      SqlAsyncQueryDetails from,
      SqlAsyncQueryDetails to
  ) throws JsonProcessingException
  {
    final String sql = StringUtils.format(
        "UPDATE %s SET state_payload = :to, metadata_payload = :metadata_payload, state_payload_sha1 = :to_sha1 WHERE id = :id AND state_payload_sha1 = :from_sha1",
        tableConfig.getSqlAsyncQueriesTable()
    );
    final byte[] serializedTo = jsonMapper.writeValueAsBytes(to);
    final int updated = handle
        .createStatement(sql)
        .bind("id", to.getAsyncResultId())
        .bind("to", serializedTo)
        .bind(
            "metadata_payload",
            jsonMapper.writeValueAsBytes(new SqlAsyncQueryMetadata(DateTimes.nowUtc().getMillis()))
        )
        .bind("to_sha1", sha1(serializedTo))
        .bind("from_sha1", sha1(jsonMapper.writeValueAsBytes(from)))
        .execute();

    return updated == 1;
  }

  private long computeResultLengthSumWithHandle(Handle handle, String sql)
  {
    return handle.createQuery(sql)
                 .map((index, r, ctx) -> {
                   try {
                     return jsonMapper.readValue(r.getBytes("state_payload"), SqlAsyncQueryDetails.class);
                   }
                   catch (IOException e) {
                     throw new RuntimeException(e);
                   }
                 })
                 .list()
                 .stream()
                 .mapToLong(
                     queryDetails -> queryDetails.getState() == State.COMPLETE ? queryDetails.getResultLength() : 0
                 )
                 .sum();
  }

  enum QueryDetailsUpdateResult
  {
    SUCCESS,
    NOT_FOUND,
    CANNOT_UPDATE
  }

  private static String sha1(byte[] serializedQueryDetails)
  {
    return BaseEncoding.base16().encode(Hashing.sha1().hashBytes(serializedQueryDetails).asBytes());
  }
}
