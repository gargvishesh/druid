/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingOutputStream;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class SqlAsyncQueryPool
{
  private static final Logger log = new Logger(SqlAsyncQueryPool.class);

  private final ExecutorService exec;
  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncResultManager resultManager;
  private final ObjectMapper jsonMapper;

  public SqlAsyncQueryPool(
      final ExecutorService exec,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      final ObjectMapper jsonMapper
  )
  {
    this.exec = exec;
    this.metadataManager = metadataManager;
    this.resultManager = resultManager;
    this.jsonMapper = jsonMapper;
  }

  public SqlAsyncQueryDetails execute(
      final String asyncResultId,
      final SqlQuery sqlQuery,
      final SqlLifecycle lifecycle,
      final String remoteAddr
  ) throws IOException, AsyncQueryAlreadyExistsException
  {
    // TODO(gianm): Document precondition: lifecycle must be in AUTHORIZED state. Validate, too?
    final String sqlQueryId = lifecycle.getPlannerContext().getSqlQueryId();

    final SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew(
        asyncResultId,
        lifecycle.getPlannerContext().getAuthenticationResult().getIdentity(),
        sqlQuery.getResultFormat()
    );

    metadataManager.addNewQuery(queryDetails);

    exec.submit(
        () -> {
          final String currThreadName = Thread.currentThread().getName();

          try {
            Thread.currentThread().setName(StringUtils.format("sql-async[%s]", sqlQueryId));

            metadataManager.updateQueryDetails(queryDetails.toRunning());

            // TODO(gianm): Most of this code is copy-pasted from SqlResource
            final PlannerContext plannerContext = lifecycle.plan();
            final DateTimeZone timeZone = plannerContext.getTimeZone();

            // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
            // Also store list of all column names, for X-Druid-Sql-Columns header.
            final List<RelDataTypeField> fieldList = lifecycle.rowType().getFieldList();
            final boolean[] timeColumns = new boolean[fieldList.size()];
            final boolean[] dateColumns = new boolean[fieldList.size()];
            final String[] columnNames = new String[fieldList.size()];

            for (int i = 0; i < fieldList.size(); i++) {
              final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
              timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
              dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
              columnNames[i] = fieldList.get(i).getName();
            }

            Yielder<Object[]> yielder = Yielders.each(lifecycle.execute());

            CountingOutputStream outputStream;

            try (
                final OutputStream baseOutputStream = resultManager.writeResults(queryDetails);
                final ResultFormat.Writer writer =
                    sqlQuery.getResultFormat()
                            .createFormatter((outputStream = new CountingOutputStream(baseOutputStream)), jsonMapper)
            ) {
              writer.writeResponseStart();

              if (sqlQuery.includeHeader()) {
                writer.writeHeader(Arrays.asList(columnNames));
              }

              while (!yielder.isDone()) {
                final Object[] row = yielder.get();
                writer.writeRowStart();
                for (int i = 0; i < fieldList.size(); i++) {
                  final Object value;

                  if (row[i] == null) {
                    value = null;
                  } else if (timeColumns[i]) {
                    value = ISODateTimeFormat.dateTime().print(
                        Calcites.calciteTimestampToJoda((long) row[i], timeZone)
                    );
                  } else if (dateColumns[i]) {
                    value = ISODateTimeFormat.dateTime().print(
                        Calcites.calciteDateToJoda((int) row[i], timeZone)
                    );
                  } else {
                    value = row[i];
                  }

                  writer.writeRowField(fieldList.get(i).getName(), value);
                }
                writer.writeRowEnd();
                yielder = yielder.next(null);
              }

              writer.writeResponseEnd();
            }
            finally {
              yielder.close();
            }

            metadataManager.updateQueryDetails(queryDetails.toComplete(outputStream.getCount()));
            lifecycle.emitLogsAndMetrics(null, remoteAddr, outputStream.getCount());
          }
          catch (Exception e) {
            log.warn(e, "Failed to execute async query [%s]", sqlQueryId);
            lifecycle.emitLogsAndMetrics(e, remoteAddr, -1);

            try {
              metadataManager.updateQueryDetails(queryDetails.toError(e));
            }
            catch (Exception e2) {
              log.warn(e2, "Failed to set error for async query [%s]", sqlQueryId);
            }
          }
          finally {
            Thread.currentThread().setName(currThreadName);
          }
        }
    );

    return queryDetails;
  }
}
