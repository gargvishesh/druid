/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.input.InputSpec;
import io.imply.druid.talaria.input.StageInputSpec;
import io.imply.druid.talaria.kernel.MaxCountShuffleSpec;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ShuffleSpec;
import io.imply.druid.talaria.kernel.StageDefinition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MockQueryDefinitionBuilder
{
  static final String SHUFFLE_KEY_COLUMN = "shuffleKey";
  static final RowSignature STAGE_SIGNATURE = RowSignature.builder().add(SHUFFLE_KEY_COLUMN, ColumnType.STRING).build();

  private static final int MAX_NUM_PARTITIONS = 32;

  private final int numStages;

  // Maps a stage to all the other stages on which it has dependency, i.e. for an edge like A -> B, the adjacency list
  // would have an entry like B : [ A, ... ]
  private final Map<Integer, Set<Integer>> adjacencyList = new HashMap<>();

  // Keeps a collection of those stages that have been already defined
  private final Set<Integer> definedStages = new HashSet<>();

  // Query definition builder corresponding to this mock builder
  private final QueryDefinitionBuilder queryDefinitionBuilder = QueryDefinition.builder();


  public MockQueryDefinitionBuilder(final int numStages)
  {
    this.numStages = numStages;
  }

  public MockQueryDefinitionBuilder addVertex(final int outEdge, final int inEdge)
  {
    Preconditions.checkArgument(
        outEdge < numStages,
        "vertex number can only be from 0 to one less than the total number of stages"
    );

    Preconditions.checkArgument(
        inEdge < numStages,
        "vertex number can only be from 0 to one less than the total number of stages"
    );

    Preconditions.checkArgument(
        !definedStages.contains(inEdge),
        StringUtils.format("%s is already defined, cannot create more connections from it", inEdge)
    );

    Preconditions.checkArgument(
        !definedStages.contains(outEdge),
        StringUtils.format("%s is already defined, cannot create more connections to it", outEdge)
    );

    adjacencyList.computeIfAbsent(inEdge, k -> new HashSet<>()).add(outEdge);
    return this;
  }

  public MockQueryDefinitionBuilder defineStage(
      int stageNumber,
      boolean shuffling,
      int maxWorkers
  )
  {
    Preconditions.checkArgument(
        stageNumber < numStages,
        "stageNumber should be between 0 and total stages - 1"
    );
    Preconditions.checkArgument(
        !definedStages.contains(stageNumber),
        StringUtils.format("%d is already defined", stageNumber)
    );
    definedStages.add(stageNumber);

    ShuffleSpec shuffleSpec;

    if (shuffling) {
      shuffleSpec = new MaxCountShuffleSpec(
          new ClusterBy(
              ImmutableList.of(
                  new ClusterByColumn(SHUFFLE_KEY_COLUMN, false)
              ),
              0
          ),
          MAX_NUM_PARTITIONS,
          false
      );
    } else {
      shuffleSpec = null;
    }

    final List<InputSpec> inputSpecs =
        adjacencyList.getOrDefault(stageNumber, new HashSet<>())
                     .stream()
                     .map(StageInputSpec::new).collect(Collectors.toList());

    if (inputSpecs.isEmpty()) {
      inputSpecs.add(new ControllerTestInputSpec());
    }

    queryDefinitionBuilder.add(
        StageDefinition.builder(stageNumber)
                       .inputs(inputSpecs)
                       .processorFactory(Mockito.mock(FrameProcessorFactory.class))
                       .shuffleSpec(shuffleSpec)
                       .signature(RowSignature.builder().add(SHUFFLE_KEY_COLUMN, ColumnType.STRING).build())
                       .maxWorkerCount(maxWorkers)
    );

    return this;
  }

  public MockQueryDefinitionBuilder defineStage(int stageNumber, boolean shuffling)
  {
    return defineStage(stageNumber, shuffling, 1);
  }

  public MockQueryDefinitionBuilder defineStage(int stageNumber)
  {
    return defineStage(stageNumber, false);
  }

  public QueryDefinitionBuilder getQueryDefinitionBuilder()
  {
    if (!verifyIfAcyclic()) {
      throw new ISE("The stages of the query form a cycle. Cannot create a query definition builder");
    }
    for (int i = 0; i < numStages; ++i) {
      if (!definedStages.contains(i)) {
        defineStage(i);
      }
    }
    return queryDefinitionBuilder;
  }

  /**
   * Perform a basic check that the query definition that the user is trying to build is acyclic indeed. This method
   * is not required in the source code because the DAG there is created by query toolkits
   */
  private boolean verifyIfAcyclic()
  {
    Map<Integer, StageState> visited = new HashMap<>();

    for (int i = 0; i < numStages; i++) {
      if (!checkAcyclic(i, visited)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks for graph cycles using DFS
   */
  private boolean checkAcyclic(int node, Map<Integer, StageState> visited)
  {
    StageState state = visited.getOrDefault(node, StageState.NEW);
    if (state == StageState.VISITED) {
      return true;
    }
    if (state == StageState.VISITING) {
      return false;
    } else {
      visited.put(node, StageState.VISITING);
      for (int neighbour : adjacencyList.getOrDefault(node, Collections.emptySet())) {
        if (!checkAcyclic(neighbour, visited)) {
          return false;
        }
      }
      visited.put(node, StageState.VISITED);
      return true;
    }
  }

  private enum StageState
  {
    NEW,
    VISITING,
    VISITED
  }
}
