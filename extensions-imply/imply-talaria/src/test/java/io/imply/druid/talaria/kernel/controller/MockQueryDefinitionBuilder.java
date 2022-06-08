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
import io.imply.druid.talaria.kernel.MaxCountShuffleSpec;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ShuffleSpec;
import io.imply.druid.talaria.kernel.StageDefinition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockQueryDefinitionBuilder
{
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

  public MockQueryDefinitionBuilder addVertices(final List<Pair<Integer, Integer>> vertices)
  {
    for (Pair<Integer, Integer> vertex : vertices) {
      Preconditions.checkNotNull(vertex.lhs);
      Preconditions.checkNotNull(vertex.rhs);
      addVertex(vertex.lhs, vertex.rhs);
    }
    return this;
  }

  public MockQueryDefinitionBuilder defineStage(int stageNumber, boolean shuffling, int maxWorkers)
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
                  new ClusterByColumn("dummy", false)
              ),
              1
          ),
          2,
          false
      );
    } else {
      shuffleSpec = new MaxCountShuffleSpec(ClusterBy.none(), 1, false);
    }

    queryDefinitionBuilder.add(
        StageDefinition.builder(stageNumber)
                       .inputStages(adjacencyList.getOrDefault(stageNumber, new HashSet<>())
                                                 .stream()
                                                 .mapToInt(Integer::intValue)
                                                 .toArray())
                       .processorFactory(Mockito.mock(FrameProcessorFactory.class))
                       .shuffleSpec(shuffleSpec)
                       .signature(RowSignature.builder().add("dummy", ColumnType.STRING).build())
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
    // TODO: Implementation
    return true;
  }
}
