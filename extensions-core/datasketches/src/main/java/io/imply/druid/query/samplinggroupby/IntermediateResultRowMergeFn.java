/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.ResultRow;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BinaryOperator;

/**
 * Merge function used by {@link org.apache.druid.query.ResultMergeQueryRunner} to merge two duplicate groups into one
 */
public class IntermediateResultRowMergeFn implements BinaryOperator<ResultRow>
{
  private final SamplingGroupByQuery query;

  public IntermediateResultRowMergeFn(SamplingGroupByQuery query)
  {
    this.query = query;
  }

  @Override
  @Nullable
  public ResultRow apply(@Nullable ResultRow arg1, @Nullable ResultRow arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }

    // Take min of the theta for the two resultRows
    int thetaColmunIndex = query.getIntermediateResultRowThetaColumnIndex();
    arg1.set(thetaColmunIndex, Math.min(arg1.getLong(thetaColmunIndex), arg1.getLong(thetaColmunIndex)));

    // Add aggregations.
    int aggregatorStart = query.getIntermediateResultRowAggregatorStart();
    List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      AggregatorFactory aggregatorFactory = aggregatorSpecs.get(i);
      int rowIndex = aggregatorStart + i;
      arg1.set(rowIndex, aggregatorFactory.combine(arg1.get(rowIndex), arg2.get(rowIndex)));
    }

    return arg1;
  }
}
