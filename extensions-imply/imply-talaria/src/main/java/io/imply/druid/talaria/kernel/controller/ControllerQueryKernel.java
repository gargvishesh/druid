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
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO(gianm): Put something in here to make sure that limited-size in-memory channels do not block
 *   (readers must be spun up at the same time as writers)
 */
public class ControllerQueryKernel
{
  private final QueryDefinition queryDef;
  private final Queue<StageId> stageQueue;
  private final Map<StageId, ControllerStageKernel> stageTrackers;

  public ControllerQueryKernel(final QueryDefinition queryDef)
  {
    this.queryDef = queryDef;
    this.stageQueue = buildStageQueue(queryDef);
    this.stageTrackers = new HashMap<>();
  }

  public List<ControllerStageKernel> getNewStageKernels()
  {
    createNewKernels();
    return stageTrackers.values()
                        .stream()
                        .filter(controllerStageKernel -> controllerStageKernel.getPhase() == ControllerStagePhase.NEW)
                        .collect(Collectors.toList());
  }

  public List<ControllerStageKernel> getActiveStageKernels()
  {
    return ImmutableList.copyOf(stageTrackers.values());
  }

  public ControllerStageKernel getStageKernel(final StageId id)
  {
    final ControllerStageKernel tracker = stageTrackers.get(id);

    if (tracker == null) {
      throw new ISE("No such stage [%s]", id);
    }

    return tracker;
  }

  public ControllerStageKernel getStageKernel(final int stageNumber)
  {
    return getStageKernel(new StageId(queryDef.getQueryId(), stageNumber));
  }

  public boolean isDone()
  {
    return Optional.ofNullable(stageTrackers.get(queryDef.getFinalStageDefinition().getId()))
                   .filter(tracker -> tracker.getPhase() == ControllerStagePhase.RESULTS_COMPLETE)
                   .isPresent()
           || stageTrackers.values().stream().anyMatch(tracker -> tracker.getPhase() == ControllerStagePhase.FAILED);
  }

  public boolean isSuccess()
  {
    return stageTrackers.size() == queryDef.getStageDefinitions().size()
           && stageTrackers.values()
                           .stream()
                           .allMatch(tracker -> tracker.getPhase() == ControllerStagePhase.RESULTS_COMPLETE);
  }

  public List<WorkOrder> createWorkOrders(
      final int stageNumber,
      @Nullable final List<Object> extraInfos
  )
  {
    final List<WorkOrder> retVal = new ArrayList<>();
    final ControllerStageKernel stageKernel = getStageKernel(stageNumber);

    List<ReadablePartitions> workerInputs = stageKernel.getWorkerInputs();
    for (int workerNumber = 0; workerNumber < workerInputs.size(); workerNumber++) {
      final Object extraInfo = extraInfos != null ? extraInfos.get(workerNumber) : null;

      //noinspection unchecked
      final ExtraInfoHolder<?> extraInfoHolder =
          stageKernel.getStageDefinition().getProcessorFactory().makeExtraInfoHolder(extraInfo);

      retVal.add(
          new WorkOrder(
              queryDef,
              stageNumber,
              workerNumber,
              stageKernel.getWorkerInputs().get(workerNumber),
              extraInfoHolder
          )
      );
    }

    return retVal;
  }

  private void createNewKernels()
  {
    // TODO(gianm): Logic problem with the stage ordering here: stages with nonstreamable inputs can potentially block
    //    indpendent stages with streamable inputs

    while (!stageQueue.isEmpty()) {
      final StageId nextStage = stageQueue.peek();

      final boolean isReady =
          queryDef.getStageDefinition(nextStage)
                  .getInputStageIds()
                  .stream()
                  .allMatch(
                      inputStage ->
                          stageTrackers.containsKey(inputStage) && stageTrackers.get(inputStage).canReadResults()
                  );

      if (isReady) {
        // Remove the stage from the queue.
        stageQueue.poll();

        // Create a tracker.
        final StageDefinition stageDef = queryDef.getStageDefinition(nextStage);
        final ControllerStageKernel stageKernel = ControllerStageKernel.create(
            stageDef,
            stageDef.getInputStageIds().stream().map(stageTrackers::get).collect(Collectors.toList())
        );

        stageTrackers.put(nextStage, stageKernel);

        // Check the next stage in the queue (no break;).
      } else {
        // Don't check the next stage.
        break;
      }
    }
  }

  private static Queue<StageId> buildStageQueue(final QueryDefinition queryDefinition)
  {
    final Queue<StageId> retVal = new ArrayDeque<>();

    // Maximize parallelism by doing leaf stages first.
    final Map<StageId, Set<StageId>> outflowMap = buildStageOutflowMap(queryDefinition);
    final Map<StageId, Set<StageId>> inflowMap = buildStageInflowMap(queryDefinition);

    final Set<StageId> leafStages =
        queryDefinition.getStageDefinitions()
                       .stream()
                       .map(StageDefinition::getId)
                       .filter(stageId -> inflowMap.getOrDefault(stageId, Collections.emptySet()).isEmpty())
                       .collect(Collectors.toSet());

    while (!leafStages.isEmpty()) {
      for (StageId leafStageId : ImmutableList.copyOf(leafStages)) {
        retVal.add(leafStageId);

        final Set<StageId> outflows = outflowMap.get(leafStageId);

        if (outflows != null) {
          for (final StageId outflowStageId : outflows) {
            final Set<StageId> inflowsToOutflowStage = inflowMap.get(outflowStageId);

            if (!inflowsToOutflowStage.remove(leafStageId)) {
              // Sanity check.
              throw new ISE("Expect inflow from stage [%s] to stage [%s]", leafStageId, outflowStageId);
            }

            if (inflowsToOutflowStage.isEmpty()) {
              inflowMap.remove(outflowStageId);
              leafStages.add(outflowStageId);
            }
          }
        }

        outflowMap.remove(leafStageId);
        leafStages.remove(leafStageId);
      }
    }

    if (!outflowMap.isEmpty()) {
      // Sanity check.
      throw new ISE("Expected nil outflows");
    }

    if (!inflowMap.isEmpty()) {
      // Sanity check.
      throw new ISE("Expected nil inflows");
    }

    return retVal;
  }

  /**
   * Builds a mapping of stage -> stages that depend on that stage.
   */
  private static Map<StageId, Set<StageId>> buildStageOutflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new HashMap<>();

    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      for (final StageId inputStageId : queryDefinition.getStageDefinition(stageId).getInputStageIds()) {
        retVal.computeIfAbsent(inputStageId, ignored -> new HashSet<>()).add(stageId);
      }
    }

    return retVal;
  }

  /**
   * Builds a mapping of stage -> stages that flow *into* that stage.
   */
  private static Map<StageId, Set<StageId>> buildStageInflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new HashMap<>();

    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      for (final StageId inputStageId : queryDefinition.getStageDefinition(stageId).getInputStageIds()) {
        retVal.computeIfAbsent(stageId, ignored -> new HashSet<>()).add(inputStageId);
      }
    }

    return retVal;
  }
}
