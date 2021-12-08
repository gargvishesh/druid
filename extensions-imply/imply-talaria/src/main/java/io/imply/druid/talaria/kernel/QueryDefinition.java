/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class QueryDefinition
{
  private final Map<StageId, StageDefinition> stageDefinitions;
  private final StageId finalStage;

  private QueryDefinition(
      final Map<StageId, StageDefinition> stageDefinitions,
      final StageId finalStage
  )
  {
    this.stageDefinitions = stageDefinitions;
    this.finalStage = finalStage;
  }

  @JsonCreator
  static QueryDefinition create(@JsonProperty("stages") final List<StageDefinition> stageDefinitions)
  {
    final Map<StageId, StageDefinition> stageMap = new HashMap<>();
    final Set<StageId> nonFinalStages = new HashSet<>();
    final IntSet stageNumbers = new IntOpenHashSet();

    for (final StageDefinition stage : stageDefinitions) {
      if (!stageNumbers.add(stage.getStageNumber())) {
        throw new ISE("Cannot accept duplicate stage numbers");
      }

      stageMap.put(stage.getId(), stage);
      nonFinalStages.addAll(stage.getInputStageIds());
    }

    for (final StageId nonFinalStageId : nonFinalStages) {
      if (!stageMap.containsKey(nonFinalStageId)) {
        throw new ISE("Stage [%s] is missing a definition", nonFinalStageId);
      }
    }

    final int finalStageCandidates = stageMap.size() - nonFinalStages.size();

    if (finalStageCandidates == 1) {
      return new QueryDefinition(
          stageMap,
          Iterables.getOnlyElement(Sets.difference(stageMap.keySet(), nonFinalStages))
      );
    } else {
      throw new IAE("Must have a single final stage, but found [%d] candidates", finalStageCandidates);
    }
  }

  public static QueryDefinitionBuilder builder()
  {
    return new QueryDefinitionBuilder();
  }

  public static QueryDefinitionBuilder builder(final QueryDefinition queryDef)
  {
    return new QueryDefinitionBuilder().addAll(queryDef);
  }

  public String getQueryId()
  {
    return finalStage.getQueryId();
  }

  public StageDefinition getFinalStageDefinition()
  {
    return getStageDefinition(finalStage);
  }

  @JsonProperty("stages")
  public List<StageDefinition> getStageDefinitions()
  {
    return ImmutableList.copyOf(stageDefinitions.values());
  }

  public StageDefinition getStageDefinition(final int stageNumber)
  {
    return getStageDefinition(new StageId(getQueryId(), stageNumber));
  }

  public StageDefinition getStageDefinition(final StageId stageId)
  {
    return Preconditions.checkNotNull(stageDefinitions.get(stageId), "No stageId [%s]", stageId);
  }

  public ClusterBy getClusterByForStage(final int stageNumber)
  {
    // TODO(gianm): This logic assumes that non-shuffling stages with one input will retain the clustering of their inputs
    //    This is not always true! But I'm not sure if anything bad happens as a result. Need to double-check.
    StageDefinition stageDefPtr = getStageDefinition(stageNumber);

    while (!stageDefPtr.doesShuffle()) {
      final List<StageId> inputStageIds = stageDefPtr.getInputStageIds();

      if (inputStageIds.size() == 1) {
        stageDefPtr = getStageDefinition(Iterables.getOnlyElement(inputStageIds).getStageNumber());
      } else {
        // Zero, or more than one input: don't retain input clustering; just say there's nothing.
        return ClusterBy.none();
      }
    }

    return stageDefPtr.getShuffleSpec().get().getClusterBy();
  }

  /**
   * Returns a number that is higher than all current stage numbers.
   */
  public int getNextStageNumber()
  {
    return stageDefinitions.values().stream().mapToInt(StageDefinition::getStageNumber).max().orElse(-1) + 1;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryDefinition that = (QueryDefinition) o;
    return Objects.equals(stageDefinitions, that.stageDefinitions) && Objects.equals(finalStage, that.finalStage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageDefinitions, finalStage);
  }

  @Override
  public String toString()
  {
    return "QueryDefinition{" +
           "stageDefinitions=" + stageDefinitions +
           ", finalStage=" + finalStage +
           '}';
  }
}
