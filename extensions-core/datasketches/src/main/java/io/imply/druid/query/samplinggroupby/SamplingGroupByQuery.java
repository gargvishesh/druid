/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Supports the same parameters as provided by the normal groupBy query. The unsupported options right now are
 * ‘limitSpec’ and ‘subtotalsSpec’. The plan for ‘limitSpec’ is to be supported in the v0 merge, whereas the
 * ‘subtotalsSpec’ can be picked in v1. Further, an augmentation is the support of the ‘maxGroups’ parameter.
 * This parameter specifies the maximum number of uniformly sampled groups that the query will produce.
 *
 * Internally, the {@link SamplingGroupByQuery} on historicals is an augmentation to the current {@link GroupByQuery}.
 * To re-use the existing GroupByEngine, a GroupByQuery is produced from within the {@link SamplingGroupByQuery} object.
 * The management of that internal GroupByQuery is done via the {@link SamplingGroupByQuery} oject - where methods and members
 * referring to *intermediate* state are actually referring to the internal GroupByQuery we run on historicals.
 * The intermediate GroupByQuery has its own ordering and schema of the results - which could be overriden by
 * {@link SamplingGroupByQuery} in the broker. But currently, we don't override the results.
 * For reference, the current intermediate result row is of the format :
 * [__time [optional], _hash, dim1, dim2..., _theta, agg1, agg2..., postAggs [optional]]
 * The difference between typical ResultRow coming from a GroupBy vs the ResultRow coming from {@link SamplingGroupByQuery} is the
 * insertion of _hash column as the first dimension and _theta as the last dimension.
 */
@EverythingIsNonnullByDefault
@JsonTypeName(SamplingGroupByQuery.QUERY_TYPE)
public class SamplingGroupByQuery extends BaseQuery<ResultRow> implements Query<ResultRow>
{
  private final List<DimensionSpec> dimensions;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final @Nullable DimFilter dimFilter;
  private final @Nullable HavingSpec havingSpec;
  private final Integer maxGroups;
  private final @Nullable Ordering<ResultRow> resultOrdering;
  private final RowSignature rowSignature;
  public static final String QUERY_TYPE = "samplingGroupBy";
  public static final String INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME = "_hash";
  public static final String INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME = "_theta";
  public static final String SAMPLING_RATE_DIMESION_NAME = "_samplingRate";
  public static final DimensionSpec SAMPLING_RATE_DIMENSION_SPEC =
      new DefaultDimensionSpec(SAMPLING_RATE_DIMESION_NAME, SAMPLING_RATE_DIMESION_NAME, ColumnType.DOUBLE);
  public static final Integer MAX_GROUPS = 100;

  @JsonCreator
  public SamplingGroupByQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("dimensions") @Nullable List<DimensionSpec> dimensions,
      @JsonProperty("aggregations") @Nullable List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") @Nullable List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("filter") @Nullable DimFilter dimFilter,
      @JsonProperty("having") @Nullable HavingSpec havingSpec,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("maxGroups") @Nullable Integer maxGroups
  )
  {
    this(
        dataSource,
        querySegmentSpec,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        dimFilter,
        havingSpec,
        granularity,
        context,
        maxGroups,
        null
    );
  }

  private SamplingGroupByQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      @Nullable List<DimensionSpec> dimensions,
      @Nullable List<AggregatorFactory> aggregatorSpecs,
      @Nullable List<PostAggregator> postAggregatorSpecs,
      @Nullable DimFilter dimFilter,
      @Nullable HavingSpec havingSpec,
      Granularity granularity,
      Map<String, Object> context,
      @Nullable Integer maxGroups,
      @Nullable Ordering<ResultRow> resultOrdering
  )
  {
    super(dataSource, querySegmentSpec, false, context, granularity);
    this.dimensions = dimensions == null ? ImmutableList.of() : dimensions;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.of() : aggregatorSpecs;
    List<String> resultDimensionOutputNames = this.dimensions.stream()
                                                             .map(DimensionSpec::getOutputName)
                                                             .collect(Collectors.toList());
    resultDimensionOutputNames.addAll(ImmutableList.of(SAMPLING_RATE_DIMESION_NAME));
    this.postAggregatorSpecs = Queries.prepareAggregations(
        resultDimensionOutputNames,
        this.aggregatorSpecs,
        postAggregatorSpecs == null ? ImmutableList.of() : postAggregatorSpecs
    );
    this.dimFilter = dimFilter;
    this.havingSpec = havingSpec;
    this.maxGroups = GuavaUtils.firstNonNull(maxGroups, MAX_GROUPS);
    this.resultOrdering = resultOrdering;
    this.rowSignature = computeResultRowSignature(RowSignature.Finalization.UNKNOWN);

    verifyOutputNames(
        resultDimensionOutputNames,
        this.aggregatorSpecs,
        this.postAggregatorSpecs
    );
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Nullable
  @Override
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return SamplingGroupByQuery.QUERY_TYPE;
  }

  @Override
  public SamplingGroupByQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    Map<String, Object> context = new HashMap<>(getContext());
    context.putAll(contextOverride);
    return new SamplingGroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getDimFilter(),
        getHavingSpec(),
        getGranularity(),
        context,
        getMaxGroups()
    );
  }

  @Override
  public SamplingGroupByQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SamplingGroupByQuery(
        getDataSource(),
        spec,
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getDimFilter(),
        getHavingSpec(),
        getGranularity(),
        getContext(),
        getMaxGroups()
    );
  }

  @Override
  public SamplingGroupByQuery withDataSource(DataSource dataSource)
  {
    return new SamplingGroupByQuery(
        dataSource,
        getQuerySegmentSpec(),
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getDimFilter(),
        getHavingSpec(),
        getGranularity(),
        getContext(),
        getMaxGroups()
    );
  }

  public SamplingGroupByQuery withResultOrdering(Ordering<ResultRow> newOrdering)
  {
    return new SamplingGroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getDimFilter(),
        getHavingSpec(),
        getGranularity(),
        getContext(),
        getMaxGroups(),
        newOrdering
    );
  }

  @JsonProperty
  public Integer getMaxGroups()
  {
    return maxGroups;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @Nullable
  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @Nullable
  @JsonProperty("having")
  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @Override
  public Ordering<ResultRow> getResultOrdering()
  {
    if (resultOrdering != null) {
      return resultOrdering;
    }

    return getIntermediateRowOrdering(
        getDimensions().stream().map(DimensionSpec::getOutputType).collect(Collectors.toList()),
        getResultRowDimensionStart()
    );
  }

  private static void verifyOutputNames(
      List<String> dimensionNames,
      List<AggregatorFactory> aggregators,
      List<PostAggregator> postAggregators
  )
  {
    final Set<String> outputNames = new HashSet<>();
    for (String dimension : dimensionNames) {
      if (!outputNames.add(dimension)) {
        throw new IAE("Duplicate output name[%s]", dimension);
      }
    }

    for (AggregatorFactory aggregator : aggregators) {
      if (!outputNames.add(aggregator.getName())) {
        throw new IAE("Duplicate output name[%s]", aggregator.getName());
      }
    }

    for (PostAggregator postAggregator : postAggregators) {
      if (!outputNames.add(postAggregator.getName())) {
        throw new IAE("Duplicate output name[%s]", postAggregator.getName());
      }
    }

    if (outputNames.contains(ColumnHolder.TIME_COLUMN_NAME)) {
      throw new IAE(
          "'%s' cannot be used as an output name for dimensions, aggregators, or post-aggregators.",
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
  }

  public Function<Sequence<ResultRow>, Sequence<ResultRow>> makePostProcessingFn()
  {
    Function<Sequence<ResultRow>, Sequence<ResultRow>> postProcessingFn = Functions.identity();

    if (havingSpec != null) {
      ImmutableList.Builder<DimensionSpec> dimesionsWithSamplingRate = ImmutableList.builder();
      dimesionsWithSamplingRate.addAll(getDimensions()).add(SAMPLING_RATE_DIMENSION_SPEC);
      GroupByQuery groupByQuery = GroupByQuery.builder()
                                              .setDataSource(getDataSource())
                                              .setInterval(getQuerySegmentSpec())
                                              .setDimensions(dimesionsWithSamplingRate.build())
                                              .setAggregatorSpecs(getAggregatorSpecs())
                                              .setPostAggregatorSpecs(getPostAggregatorSpecs())
                                              .setDimFilter(getDimFilter())
                                              .setHavingSpec(getHavingSpec())
                                              .setGranularity(getGranularity())
                                              .setContext(getContext())
                                              .build();
      havingSpec.setQuery(groupByQuery);
      postProcessingFn = (Sequence<ResultRow> input) -> Sequences.filter(input, havingSpec::eval);
    }

    return postProcessingFn;
  }

  public SamplingGroupByQuery generateQueryWithoutHavingSpec()
  {
    return new SamplingGroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getDimFilter(),
        null,
        getGranularity(),
        getContext(),
        getMaxGroups()
    );
  }

  private RowSignature computeResultRowSignature(RowSignature.Finalization finalization)
  {
    final RowSignature.Builder builder = RowSignature.builder();

    if (!getGranularity().equals(Granularities.ALL)) {
      builder.addTimeColumn();
    }

    return builder.addDimensions(dimensions)
                  .addDimensions(ImmutableList.of(SAMPLING_RATE_DIMENSION_SPEC))
                  .addAggregators(aggregatorSpecs, finalization)
                  .addPostAggregators(postAggregatorSpecs)
                  .build();
  }

  public int getResultRowSizeWithPostAggregators()
  {
    return rowSignature.size();
  }

  /**
   * Returns the position of the first dimension in ResultRows for this query.
   */
  public int getResultRowDimensionStart()
  {
    return isIntermediateResultRowWithTimestamp() ? 1 : 0;
  }

  /**
   * Returns the position of the first aggregator in ResultRows for this query.
   */
  public int getResultRowAggregatorStart()
  {
    // adding 1 accomodates for samplingRate column
    return getResultRowDimensionStart() + getDimensions().size() + 1;
  }

  /**
   * Returns the position of the first post-aggregator in ResultRows for this query.
   */
  public int getResultRowPostAggregatorStart()
  {
    return getResultRowAggregatorStart() + aggregatorSpecs.size();
  }

  /**
   * Returns the position of the sampling rate column in ResultRows for this query.
   */
  public int getResultRowSamplingRateColumnIndex()
  {
    return getResultRowAggregatorStart() - 1;
  }

  public RowSignature getResultRowSignature()
  {
    return rowSignature;
  }


  // Intermediate ResultRow (one that's coming from historical) helper methods

  public boolean isIntermediateResultRowWithTimestamp()
  {
    return !Granularities.ALL.equals(getGranularity());
  }

  /**
   * Returns the position of the first dimension in ResultRows for this query.
   */
  public int getIntermediateResultRowDimensionStart()
  {
    // offseted bhy 1 to accomodate for hash column
    return isIntermediateResultRowWithTimestamp() ? 2 : 1;
  }

  /**
   * Returns the position of the first aggregator in ResultRows for this query.
   */
  public int getIntermediateResultRowAggregatorStart()
  {
    // adding 1 accomodates for theta column
    return getIntermediateResultRowDimensionStart() + getDimensions().size() + 1;
  }

  /**
   * Returns the position of the first post-aggregator in ResultRows for this query.
   */
  public int getIntermediateResultRowPostAggregatorStart()
  {
    return getIntermediateResultRowAggregatorStart() + aggregatorSpecs.size();
  }

  public int getIntermediateResultRowHashColumnIndex()
  {
    return isIntermediateResultRowWithTimestamp() ? 1 : 0;
  }

  public int getIntermediateResultRowThetaColumnIndex()
  {
    return getIntermediateResultRowAggregatorStart() - 1;
  }

  public Ordering<ResultRow> getIntermediateRowOrdering(List<ColumnType> columnTypes, int colStart)
  {
    final Comparator<ResultRow> timeComparator = getTimeComparator();

    if (timeComparator == null) {
      return Ordering.from((lhs, rhs) -> compareDims(columnTypes, lhs, rhs, colStart));
    } else {
      return Ordering.from(
          (lhs, rhs) -> {
            final int timeCompare = timeComparator.compare(lhs, rhs);

            if (timeCompare != 0) {
              return timeCompare;
            }

            return compareDims(columnTypes, lhs, rhs, colStart);
          }
      );
    }
  }

  private int compareDims(List<ColumnType> columnTypes, ResultRow lhs, ResultRow rhs, int colStart)
  {
    for (int i = 0; i < columnTypes.size(); i++) {
      final int dimCompare = DimensionHandlerUtils.compareObjectsAsType(
          lhs.get(colStart + i),
          rhs.get(colStart + i),
          columnTypes.get(i)
      );
      if (dimCompare != 0) {
        return dimCompare;
      }
    }

    return 0;
  }

  @Nullable
  private Comparator<ResultRow> getTimeComparator()
  {
    if (!isIntermediateResultRowWithTimestamp()) {
      return null;
    } else {
      return (ResultRow lhs, ResultRow rhs) -> Longs.compare(lhs.getLong(0), rhs.getLong(0));
    }
  }

  /**
   * Generates an intermediate GroupBy query which is used in historical query processing. The intermediate query
   * doesn't include the post aggregators present in the original query. It is ok to do so since post aggregators are
   * always processed in brokers using the original {@link SamplingGroupByQuery}.
   */
  public GroupByQuery generateIntermediateGroupByQuery()
  {
    ImmutableList.Builder<DimensionSpec> dimesionsWithHashAndTheta = ImmutableList.builder();
    dimesionsWithHashAndTheta
        .add(
            new DefaultDimensionSpec(
                INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                ColumnType.LONG
            )
        )
        .addAll(getDimensions())
        .add(
            new DefaultDimensionSpec(
                INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
                INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
                ColumnType.LONG
            ));
    return GroupByQuery.builder()
                       .setDataSource(getDataSource())
                       .setInterval(getQuerySegmentSpec())
                       .setDimensions(dimesionsWithHashAndTheta.build())
                       .setAggregatorSpecs(getAggregatorSpecs())
                       .setDimFilter(getDimFilter())
                       .setHavingSpec(getHavingSpec())
                       .setGranularity(getGranularity())
                       .setContext(getContext())
                       .build();
  }
}
