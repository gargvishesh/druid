/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class QueryDefinitionBuilder
{
  private String queryId = UUID.randomUUID().toString();
  private final List<StageDefinitionBuilder> stageBuilders = new ArrayList<>();

  /**
   * Nothing to do, but we want to avoid having a public constructor. Callers should use {@link QueryDefinition#builder()}
   * instead
   */
  QueryDefinitionBuilder()
  {
  }

  public QueryDefinitionBuilder queryId(final String queryId)
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    return this;
  }

  public QueryDefinitionBuilder add(final StageDefinitionBuilder stageBuilder)
  {
    stageBuilders.add(stageBuilder);
    return this;
  }

  public QueryDefinitionBuilder addAll(final QueryDefinition queryDef)
  {
    for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
      add(StageDefinition.builder(stageDef));
    }
    return this;
  }

  public QueryDefinitionBuilder addAll(final QueryDefinitionBuilder queryDefBuilder)
  {
    for (final StageDefinitionBuilder stageDefBuilder : queryDefBuilder.stageBuilders) {
      add(stageDefBuilder);
    }
    return this;
  }

  /**
   * Returns a number that is higher than all current stage numbers.
   */
  public int getNextStageNumber()
  {
    return stageBuilders.stream().mapToInt(StageDefinitionBuilder::getStageNumber).max().orElse(-1) + 1;
  }

  /**
   * Returns an array of all stage numbers that are not used as inputs to any other stages.
   *
   * When {@link #build()} is called, it is expected that this array will have exactly one element. However, it can
   * have a different number while a query is in the process of being built up.
   */
  public int[] getFinalStageNumbers()
  {
    // Sorted set isn't strictly necessary, but it looks nicer for APIs that return stage numbers.
    final IntSortedSet finalStageNumbers = new IntRBTreeSet();

    for (final StageDefinitionBuilder stageBuilder : stageBuilders) {
      finalStageNumbers.add(stageBuilder.getStageNumber());
    }

    for (final StageDefinitionBuilder stageBuilder : stageBuilders) {
      for (final int inputStageNumber : stageBuilder.getInputStageNumbers()) {
        finalStageNumbers.remove(inputStageNumber);
      }
    }

    return finalStageNumbers.toIntArray();
  }

  public QueryDefinition build()
  {
    // TODO(gianm): Sanity check: POSTSHUFFLE stages must have same clusterBy as previous stage

    final List<StageDefinition> stageDefinitions =
        stageBuilders.stream().map(builder -> builder.build(queryId)).collect(Collectors.toList());

    return QueryDefinition.create(stageDefinitions);
  }
}
