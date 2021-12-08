/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.indexing.DataSourceTalariaDestination;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.QueryFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TalariaQueryMaker implements QueryMaker
{
  private static final String CTX_DESTINATION = "talariaDestination";
  private static final String CTX_MAX_NUM_CONCURRENT_SUB_TASKS = "talariaNumTasks";
  private static final String CTX_ROWS_PER_SEGMENT = "talariaRowsPerSegment";
  private static final String CTX_ROWS_IN_MEMORY = "talariaRowsInMemory";
  private static final String CTX_REPLACE_TIME_CHUNKS = "talariaReplaceTimeChunks";

  private static final String DESTINATION_DATASOURCE = "dataSource";
  private static final String DESTINATION_REPORT = "taskReport";
  private static final String DESTINATION_EXTERNAL = "external";

  private static final String DEFAULT_SEGMENT_GRANULARITY = GranularityType.ALL.name();
  private static final int DEFAULT_MAX_NUM_CONCURRENT_SUB_TASKS = 1;
  private static final int DEFAULT_ROWS_PER_SEGMENT = 3000000;

  // Lower than the default to minimize the impact of per-row overheads that are not accounted for by
  // OnheapIncrementalIndex. For example: overheads related to creating bitmaps during persist.
  private static final int DEFAULT_ROWS_IN_MEMORY = 150000;

  private final String targetDataSource;
  private final IndexingServiceClient indexingServiceClient;
  private final PlannerContext plannerContext;
  private final ObjectMapper jsonMapper;
  private final List<Pair<Integer, String>> fieldMapping;
  private final RelDataType resultType;

  TalariaQueryMaker(
      @Nullable final String targetDataSource,
      final IndexingServiceClient indexingServiceClient,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper,
      final List<Pair<Integer, String>> fieldMapping,
      final RelDataType resultType
  )
  {
    this.targetDataSource = targetDataSource;
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
    this.plannerContext = Preconditions.checkNotNull(plannerContext, "plannerContext");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.fieldMapping = Preconditions.checkNotNull(fieldMapping, "fieldMapping");
    this.resultType = Preconditions.checkNotNull(resultType, "resultType");
  }

  @Override
  public RelDataType getResultType()
  {
    return resultType;
  }

  @Override
  public boolean feature(QueryFeature feature)
  {
    switch (feature) {
      case CAN_RUN_TIMESERIES:
      case CAN_RUN_TOPN:
        return false;
      case CAN_READ_EXTERNAL_DATA:
      case SCAN_CAN_ORDER_BY_NON_TIME:
        return true;
      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public Sequence<Object[]> runQuery(final DruidQuery druidQuery)
  {
    // druid-sql module does not depend on druid-indexing-service, so we must create the task from scratch.
    final String taskId;

    if (targetDataSource == null) {
      taskId = StringUtils.format("talaria-sql-%s", plannerContext.getSqlQueryId());
    } else {
      taskId = StringUtils.format("talaria-sql-%s-%s", targetDataSource, plannerContext.getSqlQueryId());
    }

    final String ctxDestination =
        DimensionHandlerUtils.convertObjectToString(plannerContext.getQueryContext().get(CTX_DESTINATION));

    // TODO(gianm): better error messages for bad parameters
    final Object segmentGranularity =
        Optional.ofNullable(plannerContext.getQueryContext().get(QueryKitUtils.CTX_SEGMENT_GRANULARITY))
                .orElse(DEFAULT_SEGMENT_GRANULARITY);

    final long maxNumConcurrentSubTasks =
        Optional.ofNullable(plannerContext.getQueryContext().get(CTX_MAX_NUM_CONCURRENT_SUB_TASKS))
                .map(DimensionHandlerUtils::convertObjectToLong)
                .map(Ints::checkedCast)
                .orElse(DEFAULT_MAX_NUM_CONCURRENT_SUB_TASKS);

    final int rowsPerSegment =
        Optional.ofNullable(plannerContext.getQueryContext().get(CTX_ROWS_PER_SEGMENT))
                .map(DimensionHandlerUtils::convertObjectToLong)
                .map(Ints::checkedCast)
                .orElse(DEFAULT_ROWS_PER_SEGMENT);

    final int rowsInMemory =
        Optional.ofNullable(plannerContext.getQueryContext().get(CTX_ROWS_IN_MEMORY))
                .map(DimensionHandlerUtils::convertObjectToLong)
                .map(Ints::checkedCast)
                .orElse(DEFAULT_ROWS_IN_MEMORY);

    DimensionHandlerUtils.convertObjectToLong(
        plannerContext.getQueryContext().get(CTX_ROWS_IN_MEMORY)
    );

    final List<Interval> replaceTimeChunks =
        Optional.ofNullable(plannerContext.getQueryContext().get(CTX_REPLACE_TIME_CHUNKS))
                .map(
                    s -> {
                      if ("all".equals(s)) {
                        return Intervals.ONLY_ETERNITY;
                      } else {
                        final String[] parts = ((String) s).split("\\s*,\\s*");
                        final List<Interval> intervals = new ArrayList<>();

                        for (final String part : parts) {
                          intervals.add(Intervals.of(part));
                        }

                        return intervals;
                      }
                    }
                )
                .orElse(null);

    final DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(Ints.checkedCast(rowsPerSegment), null);

    final List<String> sqlTypeNames = new ArrayList<>();
    final List<Map<String, Object>> columnMappings = new ArrayList<>();

    String timeColumnName = null; // For later.

    for (final Pair<Integer, String> entry : fieldMapping) {
      // Note: SQL generally allows output columns to be duplicates, but ImplyQueryMakerFactory.validateNoDuplicateAliases
      // will prevent duplicate output columns from appearing here. So no need to worry about it.

      final String queryColumn = druidQuery.getOutputRowSignature().getColumnName(entry.getKey());
      final String outputColumns = entry.getValue();

      if (outputColumns.equals(ColumnHolder.TIME_COLUMN_NAME)) {
        timeColumnName = queryColumn;
      }

      sqlTypeNames.add(
          druidQuery.getOutputRowType().getFieldList().get(entry.getKey()).getType().getSqlTypeName().getName()
      );
      columnMappings.add(ImmutableMap.of("queryColumn", queryColumn, "outputColumn", outputColumns));
    }

    final Map<String, Object> tuningConfig =
        ImmutableMap.<String, Object>builder()
                    .put("type", "index_parallel")
                    .put("partitionsSpec", jsonMapper.convertValue(partitionsSpec, Object.class))
                    .put("maxNumConcurrentSubTasks", maxNumConcurrentSubTasks)
                    .put("maxRetry", 1) // Retries are not yet handled properly
                    .put("maxRowsInMemory", rowsInMemory)
                    .build();

    final Map<String, Object> destinationSpec;

    if (targetDataSource != null) {
      if (ctxDestination != null && !DESTINATION_DATASOURCE.equals(ctxDestination)) {
        throw new IAE("Cannot INSERT with destination [%s]", ctxDestination);
      }

      destinationSpec = jsonMapper.convertValue(
          new DataSourceTalariaDestination(
              targetDataSource,
              jsonMapper.convertValue(segmentGranularity, Granularity.class),
              replaceTimeChunks
          ),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
    } else {
      if (ctxDestination != null
          && !DESTINATION_EXTERNAL.equals(ctxDestination)
          && !DESTINATION_REPORT.equals(ctxDestination)) {
        throw new IAE("Cannot SELECT with destination [%s]", ctxDestination);
      }

      destinationSpec =
          ImmutableMap.<String, Object>builder()
                      .put("type", ctxDestination != null ? ctxDestination : DESTINATION_REPORT)
                      .build();
    }

    // Add time column to context, if it exists.
    final Map<String, Object> timeColumnContext = new HashMap<>();
    if (timeColumnName != null) {
      timeColumnContext.put(QueryKitUtils.CTX_TIME_COLUMN_NAME, timeColumnName);
    }

    //noinspection unchecked
    final Map<String, Object> querySpec =
        ImmutableMap.<String, Object>builder()
                    .put(
                        "query",
                        jsonMapper.convertValue(
                            druidQuery.getQuery().withOverriddenContext(timeColumnContext),
                            Object.class
                        )
                    )
                    .put("columnMappings", columnMappings)
                    .put("destination", destinationSpec)
                    .put("tuningConfig", tuningConfig)
                    .build();

    final Map<String, Object> taskSpec =
        ImmutableMap.<String, Object>builder()
                    .put("type", "talaria0")
                    .put("id", taskId)
                    .put("spec", querySpec)
                    .put("sqlQuery", plannerContext.getSql())
                    .put("sqlQueryContext", plannerContext.getQueryContext())
                    .put("sqlTypeNames", sqlTypeNames)
                    .build();

    indexingServiceClient.runTask(taskId, taskSpec);

    return Sequences.simple(Collections.singletonList(new Object[]{taskId}));
  }
}
