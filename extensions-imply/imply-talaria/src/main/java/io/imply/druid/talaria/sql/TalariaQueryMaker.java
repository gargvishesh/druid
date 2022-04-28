/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.indexing.DataSourceTalariaDestination;
import io.imply.druid.talaria.indexing.TalariaInsertContextKeys;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Grouping;
import org.apache.druid.sql.calcite.run.QueryFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TalariaQueryMaker implements QueryMaker
{
  public static final String CTX_MAX_NUM_CONCURRENT_SUB_TASKS = "talariaNumTasks";
  public static final String CTX_REPLACE_TIME_CHUNKS = "talariaReplaceTimeChunks";

  private static final String CTX_DESTINATION = "talariaDestination";
  private static final String CTX_ROWS_PER_SEGMENT = "talariaRowsPerSegment";
  private static final String CTX_ROWS_IN_MEMORY = "talariaRowsInMemory";
  private static final String CTX_FINALIZE_AGGREGATIONS = "talariaFinalizeAggregations";

  private static final String DESTINATION_DATASOURCE = "dataSource";
  private static final String DESTINATION_REPORT = "taskReport";
  private static final String DESTINATION_EXTERNAL = "external";

  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;
  private static final int DEFAULT_MAX_NUM_CONCURRENT_SUB_TASKS = 1;
  private static final int DEFAULT_ROWS_PER_SEGMENT = 3000000;

  // Lower than the default to minimize the impact of per-row overheads that are not accounted for by
  // OnheapIncrementalIndex. For example: overheads related to creating bitmaps during persist.
  private static final int DEFAULT_ROWS_IN_MEMORY = 100000;

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

    Object segmentGranularity;
    try {
      // TODO(gianm): better error messages for bad parameters
      segmentGranularity = Optional.ofNullable(plannerContext.getQueryContext()
                                                             .get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY))
                                   .orElse(jsonMapper.writeValueAsString(DEFAULT_SEGMENT_GRANULARITY));
    }
    catch (JsonProcessingException e) {
      throw new ISE("Unable to serialize default segment granularity.");
    }

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

    final boolean finalizeAggregations = isFinalizeAggregations(plannerContext);

    final List<Interval> replaceTimeChunks =
        Optional.ofNullable(plannerContext.getQueryContext().get(CTX_REPLACE_TIME_CHUNKS))
                .map(
                    s -> {
                      if (s instanceof String && "all".equals(StringUtils.toLowerCase((String) s))) {
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

    // For assistance computing return types if !finalizeAggregations.
    final Map<String, ColumnType> aggregationIntermediateTypeMap =
        finalizeAggregations ? null /* Not needed */ : buildAggregationIntermediateTypeMap(druidQuery);

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

      final SqlTypeName sqlTypeName;

      if (!finalizeAggregations && aggregationIntermediateTypeMap.containsKey(queryColumn)) {
        final ColumnType druidType = aggregationIntermediateTypeMap.get(queryColumn);
        sqlTypeName = new RowSignatures.ComplexSqlType(SqlTypeName.OTHER, druidType, true).getSqlTypeName();
      } else {
        sqlTypeName = druidQuery.getOutputRowType().getFieldList().get(entry.getKey()).getType().getSqlTypeName();
      }

      sqlTypeNames.add(sqlTypeName.getName());
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

      Granularity segmentGranularityObject;
      try {
        segmentGranularityObject = jsonMapper.readValue((String) segmentGranularity, Granularity.class);
      }
      catch (Exception e) {
        throw new ISE("Unable to convert %s to a segment granularity", segmentGranularity);
      }

      final List<String> segmentSortOrder = TalariaInsertContextKeys.decodeSortOrder(
          (String) plannerContext.getQueryContext().get(TalariaInsertContextKeys.CTX_SORT_ORDER)
      );

      validateSegmentSortOrder(
          segmentSortOrder,
          fieldMapping.stream().map(f -> f.right).collect(Collectors.toList())
      );

      destinationSpec = jsonMapper.convertValue(
          new DataSourceTalariaDestination(
              targetDataSource,
              segmentGranularityObject,
              segmentSortOrder,
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

    final Map<String, Object> nativeQueryContextOverrides = new HashMap<>();

    // Add time column to native query context, if it exists.
    if (timeColumnName != null) {
      nativeQueryContextOverrides.put(QueryKitUtils.CTX_TIME_COLUMN_NAME, timeColumnName);
    }

    // Add appropriate finalization to native query context.
    nativeQueryContextOverrides.put(QueryContexts.FINALIZE_KEY, finalizeAggregations);

    //noinspection unchecked
    final Map<String, Object> querySpec =
        ImmutableMap.<String, Object>builder()
                    .put(
                        "query",
                        jsonMapper.convertValue(
                            druidQuery.getQuery().withOverriddenContext(nativeQueryContextOverrides),
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
                    .put("sqlQueryContext", plannerContext.getQueryContext().getMergedParams())
                    .put("sqlTypeNames", sqlTypeNames)
                    .build();

    indexingServiceClient.runTask(taskId, taskSpec);

    return Sequences.simple(Collections.singletonList(new Object[]{taskId}));
  }

  static boolean isFinalizeAggregations(final PlannerContext plannerContext)
  {
    return Numbers.parseBoolean(plannerContext.getQueryContext().getOrDefault(CTX_FINALIZE_AGGREGATIONS, true));
  }

  private static Map<String, ColumnType> buildAggregationIntermediateTypeMap(final DruidQuery druidQuery)
  {
    final Grouping grouping = druidQuery.getGrouping();

    if (grouping == null) {
      return Collections.emptyMap();
    }

    final Map<String, ColumnType> retVal = new HashMap<>();

    for (final AggregatorFactory aggregatorFactory : grouping.getAggregatorFactories()) {
      retVal.put(aggregatorFactory.getName(), aggregatorFactory.getIntermediateType());
    }

    return retVal;
  }

  static void validateSegmentSortOrder(final List<String> sortOrder, final Collection<String> allOutputColumns)
  {
    final Set<String> allOutputColumnsSet = new HashSet<>(allOutputColumns);

    for (final String column : sortOrder) {
      if (!allOutputColumnsSet.contains(column)) {
        throw new IAE("Column [%s] in segment sort order does not appear in the query output", column);
      }
    }

    if (sortOrder.size() > 0
        && allOutputColumns.contains(ColumnHolder.TIME_COLUMN_NAME)
        && !ColumnHolder.TIME_COLUMN_NAME.equals(sortOrder.get(0))) {
      throw new IAE("Segment sort order must begin with column [%s]", ColumnHolder.TIME_COLUMN_NAME);
    }
  }
}
