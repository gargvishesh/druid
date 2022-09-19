/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import io.imply.druid.query.samplinggroupby.SamplingRateAggregatorFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidConvention;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Physical planning node for SamplingGroupByQuery. It tries to plan the samplingGroupBy query as :
 * ( ( (inner group by query) + sampling ) + outer query ops like projection or more outer aggregation ).
 * The phyiscal node replaces the dummy SAMPLING_RATE aggregator with a post aggregator on _samplingRate column which
 * contains the sampling rate of the results. Further, the inner query itself is converted to a SamplingGroupByQuery.
 */
public class DruidSamplingGroupByQueryRel extends DruidRel<DruidSamplingGroupByQueryRel>
{
  private final PartialDruidQuery partialQuery;
  private final Sample sample;
  private RelNode sourceRel;

  private DruidSamplingGroupByQueryRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode sourceRel,
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext,
      Sample sample
  )
  {
    super(cluster, traitSet, plannerContext);
    this.sourceRel = sourceRel;
    this.partialQuery = partialQuery;
    this.sample = sample;
  }

  public static DruidSamplingGroupByQueryRel create(
      DruidRel sourceRel,
      PartialDruidQuery partialQuery,
      Sample sample
  )
  {
    return new DruidSamplingGroupByQueryRel(
        sourceRel.getCluster(),
        sourceRel.getTraitSet().plusAll(partialQuery.getRelTraits()),
        sourceRel,
        partialQuery,
        sourceRel.getPlannerContext(),
        sample
    );
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidSamplingGroupByQueryRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidSamplingGroupByQueryRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        sourceRel,
        newQueryBuilder,
        getPlannerContext(),
        sample
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    // Must finalize aggregations on subqueries.
    final DruidQuery subQuery = ((DruidRel) sourceRel).toDruidQuery(true);
    final RowSignature sourceRowSignature = subQuery.getOutputRowSignature();
    // throw with some error message for un-plannable queries
    switch (subQuery.getQuery().getType()) {
      case Query.TIMESERIES:
        throw new ISE("Sampling query contains either no grouping columns or just groups on __time column. "
                      + "In both cases, sampling is not supported since it is found not to be as useful.");
      case Query.TOPN:
        throw new ISE("Sampling query is trying to do a TopN query. In that case, sampling is not supported "
                      + "since the query itself contains a limit and an ordering. Please either remove the limit and "
                      + "ordering or avoid using sampling query.");
      case Query.GROUP_BY:
        break;
      default:
        throw new ISE("Sampling query is trying to a %s query which is unsupported", subQuery.getQuery().getType());
    }
    GroupByQuery groupByQuery = (GroupByQuery) subQuery.getQuery();

    // replacing the calls to SamplingRateAggregator with a post aggregation on _samplingRate column from the results.
    // this is done because SamplingRateAggregator is just a syntactic sugar for SQL to access the _samplingRate generated
    // by the sampling group by query
    ImmutableList.Builder<PostAggregator> samplingRatePostAggsBuilder = ImmutableList.builder();
    ImmutableList.Builder<AggregatorFactory> aggsWithoutSamplingRateBuilder = ImmutableList.builder();
    for (AggregatorFactory aggregatorFactory : groupByQuery.getAggregatorSpecs()) {
      if (aggregatorFactory instanceof SamplingRateAggregatorFactory) {
        samplingRatePostAggsBuilder.add(
            new FieldAccessPostAggregator(
                aggregatorFactory.getName(),
                SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
            )
        );
      } else {
        aggsWithoutSamplingRateBuilder.add(aggregatorFactory);
      }
    }
    if (samplingRatePostAggsBuilder.build().size() == 0) {
      throw new ISE("Expected atleast one invocation of SAMPLING_RATE() aggregation in a sampling group by query");
    }

    // append a sampling rate field access post aggs for integration
    List<PostAggregator> postAggregatorsWithSamplingRate = new ArrayList<>(groupByQuery.getPostAggregatorSpecs());
    postAggregatorsWithSamplingRate.addAll(samplingRatePostAggsBuilder.build());

    SamplingGroupByQuery samplingGroupByQuery = new SamplingGroupByQuery(
        groupByQuery.getDataSource(),
        groupByQuery.getQuerySegmentSpec(),
        groupByQuery.getDimensions(),
        groupByQuery.getVirtualColumns(),
        aggsWithoutSamplingRateBuilder.build(),
        postAggregatorsWithSamplingRate,
        groupByQuery.getDimFilter(),
        groupByQuery.getHavingSpec(),
        groupByQuery.getGranularity(),
        groupByQuery.getContext(),
        (int) sample.getSamplingParameters().getSamplingPercentage()
    );
    return partialQuery.build(
        new QueryDataSource(samplingGroupByQuery),
        sourceRowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        new QueryDataSource(
            Druids.newScanQueryBuilder().dataSource("__subquery__").eternityInterval().build()
        ),
        RowSignatures.fromRelDataType(
            sourceRel.getRowType().getFieldNames(),
            sourceRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  public DruidSamplingGroupByQueryRel asDruidConvention()
  {
    return new DruidSamplingGroupByQueryRel(
        getCluster(),
        getTraitSet().plus(DruidConvention.instance()),
        RelOptRule.convert(sourceRel, DruidConvention.instance()),
        partialQuery,
        getPlannerContext(),
        sample
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(sourceRel);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    if (ordinalInParent != 0) {
      throw new IndexOutOfBoundsException(StringUtils.format("Invalid ordinalInParent[%s]", ordinalInParent));
    }
    this.sourceRel = p;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidSamplingGroupByQueryRel(
        getCluster(),
        traitSet,
        Iterables.getOnlyElement(inputs),
        getPartialDruidQuery(),
        getPlannerContext(),
        sample
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return ((DruidRel<?>) sourceRel).getDataSourceNames();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return super.explainTerms(pw)
                .item("query", queryString)
                .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory()
                  .makeCost(partialQuery.estimateCost(), 0, 0)
                  .plus(sourceRel.computeSelfCost(planner, mq));
  }
}
