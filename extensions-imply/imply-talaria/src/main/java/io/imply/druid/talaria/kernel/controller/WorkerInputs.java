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
import com.google.common.collect.Iterables;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSpec;
import io.imply.druid.talaria.input.InputSpecSlicer;
import io.imply.druid.talaria.input.NilInputSlice;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Represents assignment of {@link InputSlice} to workers.
 */
public class WorkerInputs
{
  // Worker number -> input number -> input slice.
  private final Int2ObjectMap<List<InputSlice>> assignmentsMap;

  private WorkerInputs(final Int2ObjectMap<List<InputSlice>> assignmentsMap)
  {
    this.assignmentsMap = assignmentsMap;
  }

  /**
   * Create worker assignments for a stage.
   */
  public static WorkerInputs create(
      final StageDefinition stageDef,
      final Int2IntMap stageWorkerCountMap,
      final InputSpecSlicer slicer,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    // Split each inputSpec and assign to workers. This list maps worker number -> input number -> input slice.
    final Int2ObjectMap<List<InputSlice>> assignmentsMap = new Int2ObjectAVLTreeMap<>();
    final int numInputs = stageDef.getInputSpecs().size();

    if (numInputs == 0) {
      // No inputs: run a single worker. (It might generate some data out of nowhere.)
      assignmentsMap.put(0, Collections.singletonList(NilInputSlice.INSTANCE));
      return new WorkerInputs(assignmentsMap);
    }

    // Assign input slices to workers.
    for (int inputNumber = 0; inputNumber < numInputs; inputNumber++) {
      final InputSpec inputSpec = stageDef.getInputSpecs().get(inputNumber);

      if (stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
        // Broadcast case: send everything everywhere.
        final List<InputSlice> broadcastSlices = slicer.sliceStatic(inputSpec, 1);
        final InputSlice broadcastSlice = broadcastSlices.isEmpty()
                                          ? NilInputSlice.INSTANCE
                                          : Iterables.getOnlyElement(broadcastSlices);

        for (int workerNumber = 0; workerNumber < stageDef.getMaxWorkerCount(); workerNumber++) {
          assignmentsMap.computeIfAbsent(
              workerNumber,
              ignored -> Arrays.asList(new InputSlice[numInputs])
          ).set(inputNumber, broadcastSlice);
        }
      } else {
        // Non-broadcast case: split slices across workers.
        final List<InputSlice> slices = assignmentStrategy.assign(stageDef, inputSpec, stageWorkerCountMap, slicer);

        // Flip the slices, so it's worker number -> slices for that worker.
        for (int workerNumber = 0; workerNumber < slices.size(); workerNumber++) {
          assignmentsMap.computeIfAbsent(
              workerNumber,
              ignored -> Arrays.asList(new InputSlice[numInputs])
          ).set(inputNumber, slices.get(workerNumber));
        }
      }
    }

    final ObjectIterator<Int2ObjectMap.Entry<List<InputSlice>>> assignmentsIterator =
        assignmentsMap.int2ObjectEntrySet().iterator();

    boolean first = true;
    while (assignmentsIterator.hasNext()) {
      final Int2ObjectMap.Entry<List<InputSlice>> entry = assignmentsIterator.next();
      final List<InputSlice> slices = entry.getValue();

      // Replace all null slices with nil slices: this way, logic later on doesn't have to deal with nulls.
      for (int inputNumber = 0; inputNumber < numInputs; inputNumber++) {
        if (slices.get(inputNumber) == null) {
          slices.set(inputNumber, NilInputSlice.INSTANCE);
        }
      }

      // Eliminate workers that have no non-nil, non-broadcast inputs. (Except the first one, because if all input
      // is nil, *some* worker has to do *something*.)
      final boolean hasNonNilNonBroadcastInput =
          IntStream.range(0, numInputs)
                   .anyMatch(i ->
                                 !slices.get(i).equals(NilInputSlice.INSTANCE)  // Non-nil
                                 && !stageDef.getBroadcastInputNumbers().contains(i) // Non-broadcast
                   );

      if (!first && !hasNonNilNonBroadcastInput) {
        assignmentsIterator.remove();
      }

      first = false;
    }

    return new WorkerInputs(assignmentsMap);
  }

  public List<InputSlice> inputsForWorker(final int workerNumber)
  {
    return Preconditions.checkNotNull(assignmentsMap.get(workerNumber), "worker [%s]", workerNumber);
  }

  public IntSet workers()
  {
    return assignmentsMap.keySet();
  }

  public int workerCount()
  {
    return assignmentsMap.size();
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
    WorkerInputs that = (WorkerInputs) o;
    return Objects.equals(assignmentsMap, that.assignmentsMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(assignmentsMap);
  }

  @Override
  public String toString()
  {
    return assignmentsMap.toString();
  }
}
