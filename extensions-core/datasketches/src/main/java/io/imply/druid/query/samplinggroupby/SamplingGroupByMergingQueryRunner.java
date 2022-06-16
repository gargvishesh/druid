/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.ResultRow;

/**
 * This class is responsible for running multiple QueryRunners in parallel in an executor and also combining their
 * results in a sorted manner. To define sorting of results, the parent class {@link ChainedExecutionQueryRunner} uses
 * the ordering of the Query object.
 * Since this merging also happens on historicals, we override the ordering with the *interdemiate* results ordering,
 * where *intermediate* results are prodcued from the GroupByQuery we run on historicals.
 */
public class SamplingGroupByMergingQueryRunner extends ChainedExecutionQueryRunner<ResultRow>
{
  public SamplingGroupByMergingQueryRunner(
      QueryProcessingPool queryProcessingPool,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<ResultRow>> queryables
  )
  {
    super(queryProcessingPool, queryWatcher, queryables);
  }

  @Override
  public Sequence<ResultRow> run(
      QueryPlus<ResultRow> queryPlus,
      ResponseContext responseContext
  )
  {
    Query<ResultRow> query = queryPlus.getQuery();
    if (!(query instanceof SamplingGroupByQuery)) {
      throw new ISE(StringUtils.format("The query must be a %s query", SamplingGroupByQuery.QUERY_TYPE));
    }
    SamplingGroupByQuery samplingGroupByQuery = (SamplingGroupByQuery) query;
    return super.run(
        queryPlus.withQuery(
            samplingGroupByQuery.withResultOrdering(
                samplingGroupByQuery.generateIntermediateGroupByQuery().getResultOrdering()
            )
        ),
        responseContext
    );
  }
}
