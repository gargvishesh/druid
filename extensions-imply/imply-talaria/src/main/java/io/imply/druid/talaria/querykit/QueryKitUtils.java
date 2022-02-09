/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ShuffleSpecFactories;
import io.imply.druid.talaria.kernel.SplitUtils;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class QueryKitUtils
{
  // TODO(gianm): use safer name for __boost column
  // TODO(gianm): add partition boosting to groupBy too (only when there is order by!)
  public static final String PARTITION_BOOST_COLUMN = "__boost";

  // TODO(gianm): use safer name for __bucket column
  public static final String SEGMENT_GRANULARITY_COLUMN = "__bucket";

  // TODO(gianm): hack alert: this is redundant to the ColumnMappings, but is here because QueryKit doesn't get those
  public static final String CTX_TIME_COLUMN_NAME = "talariaTimeColumn";

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  public static DataSourcePlan makeDataSourcePlan(
      final QueryKit queryKit,
      final String queryId,
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final DataSegmentTimelineView timelineView,
      @Nullable DimFilter filter,
      final int maxWorkerCount,
      final int minStageNumber
  )
  {
    if (dataSource instanceof TableDataSource
        || dataSource instanceof ExternalDataSource
        || dataSource instanceof InlineDataSource) {
      final List<QueryWorkerInputSpec> inputSpecs = makeLeafInputSpecs(
          dataSource,
          querySegmentSpec,
          timelineView,
          filter,
          maxWorkerCount
      );

      return new DataSourcePlan(dataSource, inputSpecs, null);
    } else if (dataSource instanceof QueryDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);

      final QueryDefinition subQueryDef = queryKit.makeQueryDefinition(
          queryId,
          ((QueryDataSource) dataSource).getQuery(),
          timelineView,
          queryKit,
          ShuffleSpecFactories.subQueryWithMaxWorkerCount(maxWorkerCount),
          maxWorkerCount,
          minStageNumber
      );

      final int stageNumber = subQueryDef.getFinalStageDefinition().getStageNumber();

      return new DataSourcePlan(
          new InputStageDataSource(stageNumber),
          makeInputSpecsForSubQuery(stageNumber, maxWorkerCount),
          QueryDefinition.builder(subQueryDef)
      );
    } else if (dataSource instanceof JoinDataSource) {
      final QueryDefinitionBuilder subQueryDefBuilder = QueryDefinition.builder();
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(dataSource);

      final DataSourcePlan basePlan = makeDataSourcePlan(
          queryKit,
          queryId,
          analysis.getBaseDataSource(),
          querySegmentSpec,
          timelineView,
          null, // TODO(gianm): pruning + JOIN needs some work to ensure filters are pushed down properly
          maxWorkerCount,
          Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber())
      );

      basePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

      DataSource newDataSource = basePlan.getNewDataSource();

      for (int i = 0; i < analysis.getPreJoinableClauses().size(); i++) {
        final PreJoinableClause clause = analysis.getPreJoinableClauses().get(i);
        final DataSourcePlan clausePlan = makeDataSourcePlan(
            queryKit,
            queryId,
            clause.getDataSource(),
            new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
            timelineView,
            null, // TODO(gianm): pruning + JOIN needs some work to ensure filters are pushed down properly
            maxWorkerCount,
            Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber())
        );

        clausePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

        newDataSource = JoinDataSource.create(
            newDataSource,
            clausePlan.getNewDataSource(),
            clause.getPrefix(),
            clause.getCondition(),
            clause.getJoinType(),
            i == 0 ? analysis.getJoinBaseTableFilter().orElse(null) : null
        );
      }

      return new DataSourcePlan(newDataSource, basePlan.getBaseInputSpecs(), subQueryDefBuilder);
    } else {
      throw new UOE("Cannot handle datasource class [%s]", dataSource.getClass().getName());
    }
  }

  /**
   * Given a leaf datasource of type "table" or "external", create a collection of {@link QueryWorkerInputSpec} that can be
   * used by {@link QueryWorkerUtils#inputIterator} on workers.
   *
   * @param dataSource       query datasource
   * @param querySegmentSpec query segment spec (intervals or segments) that will be used to prune or window the list
   *                         of returned segments
   * @param timelineView     timeline view for retrieving segment descriptors
   * @param filter           filter that can be used to prune the set of input specs
   * @param maxWorkerCount   maximum number of {@link QueryWorkerInputSpec} to generate
   */
  public static List<QueryWorkerInputSpec> makeLeafInputSpecs(
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final DataSegmentTimelineView timelineView,
      @Nullable final DimFilter filter,
      final int maxWorkerCount
  )
  {
    // TODO(gianm): Use something like SegmentWrangler?
    if (dataSource instanceof TableDataSource) {
      return makeInputSpecsForTable(
          ((TableDataSource) dataSource).getName(),
          querySegmentSpec,
          timelineView,
          filter,
          maxWorkerCount
      );
    } else if (dataSource instanceof ExternalDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      final ExternalDataSource externalDataSource = (ExternalDataSource) dataSource;
      return makeInputSpecsForInputSource(
          externalDataSource.getInputSource(),
          externalDataSource.getInputFormat(),
          externalDataSource.getSignature(),
          maxWorkerCount
      );
    } else if (dataSource instanceof InlineDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);

      // TODO(gianm): Do this less goofily
      final InlineDataSource inlineDataSource = (InlineDataSource) dataSource;
      final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
      final RowSignature signature = inlineDataSource.getRowSignature();
      final StringBuilder stringBuilder = new StringBuilder();

      try {
        for (final Object[] rowArray : inlineDataSource.getRows()) {
          final Map<String, Object> m = new HashMap<>();

          for (int i = 0; i < signature.size(); i++) {
            m.put(signature.getColumnName(i), rowArray[i]);
          }

          stringBuilder.append(jsonMapper.writeValueAsString(m)).append('\n');
        }

        final String dataString = stringBuilder.toString();
        return makeInputSpecsForInputSource(
            dataString.isEmpty() ? NilInputSource.instance() : new InlineInputSource(dataString),
            new JsonInputFormat(null, null, null),
            signature,
            maxWorkerCount
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new UOE("Cannot handle leaf dataSource class [%s]", dataSource.getClass().getName());
    }
  }

  public static Granularity getSegmentGranularityFromContext(@Nullable final Map<String, Object> context)
  {
    final Object o = context == null ? null : context.get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY);

    if (o instanceof String) {
      try {
        return OBJECT_MAPPER.readValue((String) o, Granularity.class);
      }
      catch (JsonProcessingException e) {
        throw new ISE("Invalid segment granularity [%s]", o);
      }
    } else if (o == null) {
      return Granularities.ALL;
    } else {
      throw new ISE("Invalid segment granularity [%s]", o);
    }
  }

  public static ClusterBy clusterByWithSegmentGranularity(
      final ClusterBy clusterBy,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return clusterBy;
    } else {
      final List<ClusterByColumn> newColumns = new ArrayList<>(clusterBy.getColumns().size() + 1);
      newColumns.add(new ClusterByColumn(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN, false));
      newColumns.addAll(clusterBy.getColumns());
      return new ClusterBy(newColumns, 1);
    }
  }

  public static RowSignature signatureWithSegmentGranularity(
      final RowSignature signature,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return signature;
    } else {
      if (signature.contains(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN)) {
        throw new ISE("Cannot use reserved column [%s]", QueryKitUtils.SEGMENT_GRANULARITY_COLUMN);
      }

      return RowSignature.builder()
                         .addAll(signature)
                         .add(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN, ColumnType.LONG)
                         .build();
    }
  }

  @Nullable
  public static VirtualColumn makeSegmentGranularityVirtualColumn(
      final Granularity segmentGranularity,
      final String timeColumnName
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return null;
    } else {
      // TODO(gianm): does not escape timeColumnName! does not use origin, timeZone! make the expr directly?
      return new ExpressionVirtualColumn(
          SEGMENT_GRANULARITY_COLUMN,
          StringUtils.format(
              "timestamp_floor(\"%s\", '%s')",
              timeColumnName,
              ((PeriodGranularity) segmentGranularity).getPeriod()
          ),
          ColumnType.LONG,
          new ExprMacroTable(Collections.singletonList(new TimestampFloorExprMacro()))
      );
    }
  }

  private static List<QueryWorkerInputSpec> makeInputSpecsForTable(
      final String dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final DataSegmentTimelineView timelineView,
      @Nullable final DimFilter filter,
      final int maxWorkerCount
  )
  {
    final Iterator<DataSegmentWithInterval> segmentIterator = findMatchingSegments(
        dataSource,
        querySegmentSpec,
        timelineView
    );

    final Set<DataSegmentWithInterval> prunedSegmentSet =
        DimFilterUtils.filterShards(filter, () -> segmentIterator, segment -> segment.getSegment().getShardSpec());

    final List<List<DataSegmentWithInterval>> assignments = SplitUtils.makeSplits(
        prunedSegmentSet.iterator(),
        segment -> segment.getSegment().getSize(),
        maxWorkerCount
    );

    return assignments.stream().map(QueryWorkerInputSpec::forTable).collect(Collectors.toList());
  }

  private static List<QueryWorkerInputSpec> makeInputSpecsForInputSource(
      final InputSource inputSource,
      final InputFormat inputFormat,
      final RowSignature signature,
      final int maxWorkerCount
  )
  {
    // Worker number -> input source for that worker.
    final List<InputSource> workerInputSources;

    // Figure out input splits for each worker.
    if (inputSource.isSplittable()) {
      //noinspection unchecked
      final SplittableInputSource<Object> splittableInputSource = (SplittableInputSource<Object>) inputSource;

      try {
        // TODO(gianm): Need a limit on # of files to prevent OOMing here. We are flat-out ignoring the recommendation
        //  from InputSource#createSplits to avoid materializing the list.
        workerInputSources =
            splittableInputSource.createSplits(inputFormat, new MaxNumSplitsSplitHintSpec(maxWorkerCount))
                                 .map(splittableInputSource::withSplit)
                                 .collect(Collectors.toList());
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      workerInputSources = Collections.singletonList(inputSource);
    }

    // Sanity check. It is a bug in this method if this exception is ever thrown.
    if (workerInputSources.size() > maxWorkerCount) {
      throw new ISE("Too many workers");
    }

    return IntStream.range(0, maxWorkerCount)
                    .mapToObj(
                        workerNumber -> {
                          final InputSource workerInputSource;

                          if (workerNumber < workerInputSources.size()) {
                            workerInputSource = workerInputSources.get(workerNumber);
                          } else {
                            workerInputSource = NilInputSource.instance();
                          }

                          return QueryWorkerInputSpec.forInputSource(workerInputSource, inputFormat, signature);
                        }
                    )
                    .collect(Collectors.toList());
  }

  private static List<QueryWorkerInputSpec> makeInputSpecsForSubQuery(
      final int stageNumber,
      final int maxWorkerCount
  )
  {
    final List<QueryWorkerInputSpec> retVal = new ArrayList<>();
    for (int i = 0; i < maxWorkerCount; i++) {
      retVal.add(QueryWorkerInputSpec.forSubQuery(stageNumber));
    }
    return retVal;
  }

  private static Iterator<DataSegmentWithInterval> findMatchingSegments(
      final String dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final DataSegmentTimelineView timelineView
  )
  {
    // TODO(gianm): this logic is somewhat redundant to QuerySegmentSpec impls, but it's hard to extract
    final TimelineLookup<String, DataSegment> timeline =
        timelineView.getTimeline(dataSource, querySegmentSpec.getIntervals()).orElse(null);

    if (timeline == null) {
      return Collections.emptyIterator();
    } else if (querySegmentSpec instanceof MultipleIntervalSegmentSpec) {
      return querySegmentSpec.getIntervals().stream()
                             .flatMap(interval -> timeline.lookup(interval).stream())
                             .flatMap(
                                 holder ->
                                     StreamSupport.stream(holder.getObject().spliterator(), false).map(
                                         chunk ->
                                             new DataSegmentWithInterval(chunk.getObject(), holder.getInterval())
                                     )
                             ).iterator();
    } else {
      // TODO(gianm): Support other QSSes
      throw new UOE("Cannot handle querySegmentSpec type [%s]", querySegmentSpec.getClass().getName());
    }
  }

  private static void checkQuerySegmentSpecIsEternity(
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec
  )
  {
    final boolean querySegmentSpecIsEternity =
        querySegmentSpec instanceof MultipleIntervalSegmentSpec
        && querySegmentSpec.getIntervals().equals(Intervals.ONLY_ETERNITY);

    if (!querySegmentSpecIsEternity) {
      // TODO(gianm): do something intelligent in this case? it can happen when someone filters an
      //   external or query datasource on __time
      throw new UOE(
          "Cannot filter datasource class [%s] on [%s]",
          dataSource.getClass().getName(),
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
  }

  // TODO(gianm): tests
  @VisibleForTesting
  static class MaxNumSplitsSplitHintSpec implements SplitHintSpec
  {
    private final int numSplits;

    MaxNumSplitsSplitHintSpec(final int numSplits)
    {
      this.numSplits = numSplits;

      if (numSplits <= 0) {
        throw new IAE("numSplits must be > 0, but was [%d]", numSplits);
      }
    }

    @Override
    public <T> Iterator<List<T>> split(
        final Iterator<T> inputIterator,
        final Function<T, InputFileAttribute> inputAttributeExtractor
    )
    {
      final List<List<T>> splits = SplitUtils.makeSplits(
          inputIterator,
          item -> inputAttributeExtractor.apply(item).getSize(),
          numSplits
      );

      // Skip empty lists, since InputSources cannot always make empty lists into a valid split.
      // This works fine because "makeInputSpecsForInputSource" replaces these missing lists with NilInputSource.
      return Iterators.filter(splits.iterator(), list -> !list.isEmpty());
    }
  }
}
