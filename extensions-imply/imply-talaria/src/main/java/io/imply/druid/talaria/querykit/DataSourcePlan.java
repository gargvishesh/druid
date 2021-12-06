/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.query.DataSource;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class DataSourcePlan
{
  private final DataSource newDataSource;
  private final List<QueryWorkerInputSpec> baseInputSpecs;

  @Nullable
  private final QueryDefinitionBuilder subQueryDefBuilder;

  DataSourcePlan(
      final DataSource newDataSource,
      final List<QueryWorkerInputSpec> baseInputSpecs,
      @Nullable final QueryDefinitionBuilder subQueryDefBuilder
  )
  {
    this.newDataSource = Preconditions.checkNotNull(newDataSource, "newDataSource");
    this.baseInputSpecs = Preconditions.checkNotNull(baseInputSpecs, "baseInputSpecs");
    this.subQueryDefBuilder = subQueryDefBuilder;
  }

  public DataSource getNewDataSource()
  {
    return newDataSource;
  }

  public List<QueryWorkerInputSpec> getBaseInputSpecs()
  {
    return baseInputSpecs;
  }

  public Optional<QueryDefinitionBuilder> getSubQueryDefBuilder()
  {
    return Optional.ofNullable(subQueryDefBuilder);
  }

  public int[] getInputStageNumbers()
  {
    return getSubQueryDefBuilder()
        .map(QueryDefinitionBuilder::getFinalStageNumbers)
        .orElse(IntArrays.EMPTY_ARRAY);
  }

  public int[] getBroadcastInputStageNumbers()
  {
    // Assume that everything other than the base input is broadcast.
    // TODO(gianm): validate that this is a fair assumption.
    if (baseInputSpecs.stream().noneMatch(spec -> spec.type() == QueryWorkerInputType.SUBQUERY)) {
      return getInputStageNumbers();
    } else {
      return Arrays.stream(getInputStageNumbers())
                   .filter(
                       stageNumber ->
                           baseInputSpecs.stream()
                                         .noneMatch(spec -> spec.type() == QueryWorkerInputType.SUBQUERY
                                                            && spec.getStageNumber() == stageNumber)
                   )
                   .toArray();
    }
  }

  /**
   * Returns whether this datasource must be processed by a single worker.
   */
  public boolean isSingleWorker()
  {
    return baseInputSpecs.stream().allMatch(spec -> spec.type() == QueryWorkerInputType.SUBQUERY)
           && Arrays.equals(getInputStageNumbers(), getBroadcastInputStageNumbers());
  }
}
