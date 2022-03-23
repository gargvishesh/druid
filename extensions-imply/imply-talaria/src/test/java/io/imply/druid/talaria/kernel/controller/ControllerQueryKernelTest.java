/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ControllerQueryKernelTest
{
  QueryDefinition queryDefinition;

  @Before
  public void setup()
  {
    /* outflow map
    0 -> 2
    1 -> 2, 3
    2 -> 4
    3 -> 5
    4 -> 6
    5 -> 6
    6 is the terminal stage
     */
    queryDefinition = buildMockQueryDefinition(
        ImmutableMap.<Integer, List<Integer>>builder()
                    .put(0, ImmutableList.of())
                    .put(1, ImmutableList.of())
                    .put(2, ImmutableList.of(0, 1))
                    .put(3, ImmutableList.of(1))
                    .put(4, ImmutableList.of(2))
                    .put(5, ImmutableList.of(3))
                    .put(6, ImmutableList.of(4, 5))
                    .build()
    );
  }

  /**
   * Tests creation and deletion of kernels by running through a dummy query DAG
   */
  @Test
  public void testGetNewAndEffectivelyFinishedStageKernels()
  {
    ControllerQueryKernel queryKernel = new ControllerQueryKernel(queryDefinition);

    List<StageId> newStageIds;
    List<StageId> effectivelyFinishedStageIds;

    // Initially 0, 1 should be ready since they are not dependents on any other stages
    newStageIds = queryKernel.createAndGetNewStageIds();
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(0, 1), mapStageIdsToStageNumbers(newStageIds));
    Assert.assertEquals(ImmutableSet.of(), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));

    // Mark 1 as done and fetch the new kernels. 3 should be unblocked along with 0.
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(1));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(0, 3), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));

    // Mark 3 as done and fetch the new kernels. 5 should be unblocked along with 0.
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(3));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(0, 5), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));


    // Mark 5 as done and fetch the new kernels. Only 0 is still unblocked, but 3 can now be cleaned
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(5));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(0), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(3), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));

    // Mark 0 as done and fetch the new kernels. This should unblock 2
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(0));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(2), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(3), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));

    // Mark 2 as done and fetch new kernels. This should clear up 0 and 1 alongside 3 (which is not marked as FINISHED yet)
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(2));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(4), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(0, 1, 3), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));

    // Mark 0, 1, 3 finished together
    effectivelyFinishedStageIds.forEach(stageId -> queryKernel.finishStage(stageId, true));

    // Mark 4 as done and fetch new kernels. This should unblock 6 and clear up 2
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(4));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(6), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(2), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));

    // Mark 6 as done. No more kernels left, but we can clean up 4 and 5 alongwith 2
    transitionNewToResultsComplete(queryKernel, queryKernel.getStageId(6));
    newStageIds = queryKernel.createAndGetNewStageIds();
    Assert.assertEquals(ImmutableSet.of(), mapStageIdsToStageNumbers(newStageIds));
    effectivelyFinishedStageIds = queryKernel.getEffectivelyFinishedStageIds();
    Assert.assertEquals(ImmutableSet.of(2, 4, 5), mapStageIdsToStageNumbers(effectivelyFinishedStageIds));
    effectivelyFinishedStageIds.forEach(stageId -> queryKernel.finishStage(stageId, true));
  }

  /**
   * Maps the given list of stage kernels to a set containing their stageNumbers
   */
  private Set<Integer> mapStageIdsToStageNumbers(List<StageId> stageIds)
  {
    return stageIds.stream()
                   .map(StageId::getStageNumber)
                   .collect(Collectors.toSet());
  }

  /**
   * Converts an adjacency list of a DAG representing the stage numbers of a query into a query definition. This mocks
   * some fields like processorFactory and signature
   */
  private static QueryDefinition buildMockQueryDefinition(Map<Integer, List<Integer>> adjacencyList)
  {
    QueryDefinitionBuilder queryDefinitionBuilder = QueryDefinition.builder();
    for (Map.Entry<Integer, List<Integer>> entry : adjacencyList.entrySet()) {
      queryDefinitionBuilder.add(
          StageDefinition.builder(entry.getKey())
                         .inputStages(entry.getValue().stream().mapToInt(Integer::intValue).toArray())
                         .processorFactory(Mockito.mock(FrameProcessorFactory.class))
                         .signature(Mockito.mock(RowSignature.class))
      );
    }
    return queryDefinitionBuilder.build();
  }

  private static void transitionNewToResultsComplete(final ControllerQueryKernel queryKernel, final StageId stageId)
  {
    queryKernel.startStage(stageId);
    queryKernel.setResultsCompleteForStageAndWorker(stageId, 0, new Object());
  }
}
