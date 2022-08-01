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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
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
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private final VirtualColumns virtualColumns;
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
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("aggregations") @Nullable List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") @Nullable List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("filter") @Nullable DimFilter dimFilter,
      @JsonProperty("having") @Nullable HavingSpec havingSpec,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JsonProperty("maxGroups") @Nullable Integer maxGroups
  )
  {
    this(
        dataSource,
        querySegmentSpec,
        dimensions,
        virtualColumns,
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
      @Nullable VirtualColumns virtualColumns,
      @Nullable List<AggregatorFactory> aggregatorSpecs,
      @Nullable List<PostAggregator> postAggregatorSpecs,
      @Nullable DimFilter dimFilter,
      @Nullable HavingSpec havingSpec,
      Granularity granularity,
      @Nullable Map<String, Object> context,
      @Nullable Integer maxGroups,
      @Nullable Ordering<ResultRow> resultOrdering
  )
  {
    super(dataSource, querySegmentSpec, false, context, granularity);
    this.dimensions = dimensions == null ? ImmutableList.of() : dimensions;
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
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

  public static Builder builder()
  {
    return new Builder();
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
        getVirtualColumns(),
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
        getVirtualColumns(),
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
        getVirtualColumns(),
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
        getVirtualColumns(),
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

  @JsonProperty
  @Override
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = VirtualColumns.JsonIncludeFilter.class)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
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

    return generateIntermediateGroupByQueryOrdering();
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

  @Nullable
  @Override
  public Set<String> getRequiredColumns()
  {
    return Queries.computeRequiredColumns(
        virtualColumns,
        dimFilter,
        dimensions,
        aggregatorSpecs,
        Collections.emptyList()
    );
  }

  public SamplingGroupByQuery generateQueryWithoutHavingSpec()
  {
    return new SamplingGroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensions(),
        getVirtualColumns(),
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
    return getResultRowDimensionStart() + getDimensions().size();
  }

  /**
   * Returns the position of the first post-aggregator in ResultRows for this query.
   */
  public int getResultRowPostAggregatorStart()
  {
    // adding 1 accomodates for samplingRate column
    return getResultRowAggregatorStart() + aggregatorSpecs.size() + 1;
  }

  /**
   * Returns the position of the sampling rate column in ResultRows for this query.
   */
  public int getResultRowSamplingRateColumnIndex()
  {
    return getResultRowAggregatorStart();
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

  /**
   * Provides an ordering function for intermediate groupby query results. It compares two
   * groups first on hash value of the dimensions and then the dimension values themselves.
   * @return ordering to be used for intermediate groupsby query results
   */
  public Ordering generateIntermediateGroupByQueryOrdering()
  {
    ImmutableList.Builder<DimensionSpec> dimensionsWithHash = ImmutableList.builder();
    dimensionsWithHash
        .add(
            new DefaultDimensionSpec(
                INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                ColumnType.LONG
            )
        )
        .addAll(getDimensions());
    return GroupByQuery.builder()
                       .setDataSource(getDataSource())
                       .setInterval(getQuerySegmentSpec())
                       .setDimensions(dimensionsWithHash.build())
                       .setVirtualColumns(getVirtualColumns())
                       .setAggregatorSpecs(getAggregatorSpecs())
                       .setDimFilter(getDimFilter())
                       .setHavingSpec(getHavingSpec())
                       .setGranularity(getGranularity())
                       .setContext(getContext())
                       .build()
                       .getResultOrdering();
  }

  public static class Builder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    @Nullable
    private DimFilter dimFilter;
    private Granularity granularity;
    @Nullable
    private List<DimensionSpec> dimensions;
    @Nullable
    VirtualColumns virtualColumns;
    @Nullable
    private List<AggregatorFactory> aggregatorSpecs;
    @Nullable
    private List<PostAggregator> postAggregatorSpecs;
    @Nullable
    private HavingSpec havingSpec;
    @Nullable
    private Map<String, Object> context;
    @Nullable
    private Integer maxGroups;

    public Builder()
    {
    }

    public Builder(SamplingGroupByQuery query)
    {
      dataSource = query.getDataSource();
      querySegmentSpec = query.getQuerySegmentSpec();
      dimFilter = query.getDimFilter();
      granularity = query.getGranularity();
      dimensions = query.getDimensions();
      virtualColumns = query.getVirtualColumns();
      aggregatorSpecs = query.getAggregatorSpecs();
      postAggregatorSpecs = query.getPostAggregatorSpecs();
      havingSpec = query.getHavingSpec();
      context = query.getContext();
    }

    public Builder(SamplingGroupByQuery.Builder builder)
    {
      dataSource = builder.dataSource;
      querySegmentSpec = builder.querySegmentSpec;
      dimFilter = builder.dimFilter;
      granularity = builder.granularity;
      dimensions = builder.dimensions;
      virtualColumns = builder.virtualColumns;
      aggregatorSpecs = builder.aggregatorSpecs;
      postAggregatorSpecs = builder.postAggregatorSpecs;
      havingSpec = builder.havingSpec;
      context = builder.context;
      maxGroups = builder.maxGroups;
    }

    public SamplingGroupByQuery.Builder setDataSource(String dataSource)
    {
      this.dataSource = new TableDataSource(dataSource);
      return this;
    }

    public SamplingGroupByQuery.Builder setInterval(QuerySegmentSpec interval)
    {
      return setQuerySegmentSpec(interval);
    }

    public SamplingGroupByQuery.Builder setQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
    {
      this.querySegmentSpec = querySegmentSpec;
      return this;
    }

    public SamplingGroupByQuery.Builder setGranularity(Granularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public SamplingGroupByQuery.Builder setDimensions(DimensionSpec... dimensions)
    {
      this.dimensions = new ArrayList<>(Arrays.asList(dimensions));
      return this;
    }

    public SamplingGroupByQuery.Builder setVirtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
      return this;
    }

    public SamplingGroupByQuery.Builder setAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
    {
      this.aggregatorSpecs = Lists.newArrayList(aggregatorSpecs);
      return this;
    }

    public SamplingGroupByQuery.Builder setPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
    {
      this.postAggregatorSpecs = Lists.newArrayList(postAggregatorSpecs);
      return this;
    }

    public SamplingGroupByQuery.Builder setMaxGroups(@Nullable Integer maxGroups)
    {
      this.maxGroups = maxGroups;
      return this;
    }

    public SamplingGroupByQuery.Builder copy()
    {
      return new SamplingGroupByQuery.Builder(this);
    }

    public SamplingGroupByQuery build()
    {
      return new SamplingGroupByQuery(
          dataSource,
          querySegmentSpec,
          dimensions,
          virtualColumns,
          aggregatorSpecs,
          postAggregatorSpecs,
          dimFilter,
          havingSpec,
          granularity,
          context,
          maxGroups
      );
    }
  }

  @Override
  public String toString()
  {
    return "SamplingGroupByQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", dimensions=" + getDimensions() +
           ", virtualColumns=" + getVirtualColumns() +
           ", aggregatorSpecs=" + getAggregatorSpecs() +
           ", postAggregatorSpecs=" + getPostAggregatorSpecs() +
           ", dimFilter=" + getDimFilter() +
           ", granularity=" + getGranularity() +
           ", havingSpec=" + getHavingSpec() +
           ", context=" + getContext() +
           ", maxGroups=" + getMaxGroups() +
           '}';
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final SamplingGroupByQuery that = (SamplingGroupByQuery) o;
    return Objects.equals(getDimensions(), that.getDimensions()) &&
           Objects.equals(getVirtualColumns(), that.getVirtualColumns()) &&
           Objects.equals(getAggregatorSpecs(), that.getAggregatorSpecs()) &&
           Objects.equals(getPostAggregatorSpecs(), that.getPostAggregatorSpecs()) &&
           Objects.equals(getDimFilter(), that.getDimFilter()) &&
           Objects.equals(getHavingSpec(), that.getHavingSpec()) &&
           Objects.equals(getMaxGroups(), that.getMaxGroups());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getDimFilter(),
        getHavingSpec(),
        getMaxGroups()
    );
  }
}
