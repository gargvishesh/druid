/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.worker;

/**
 * Phases that a stage can be in, as far as the worker is concerned.
 *
 * Used by {@link WorkerStageKernel}.
 */
public enum WorkerStagePhase
{
  NEW {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return false;
    }
  },

  READING_INPUT {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == NEW;
    }
  },

  PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT;
    }
  },

  PRESHUFFLE_WRITING_OUTPUT {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES;
    }
  },

  RESULTS_READY {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT || priorPhase == PRESHUFFLE_WRITING_OUTPUT;
    }
  },

  FINISHED {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == RESULTS_READY;
    }
  },

  // Something went wrong.
  FAILED {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return true;
    }
  };

  public abstract boolean canTransitionFrom(WorkerStagePhase priorPhase);
}
