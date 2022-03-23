/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Phases that a stage can be in, as far as the controller is concerned.
 *
 * Used by {@link ControllerStageKernel}.
 */
public enum ControllerStagePhase
{
  // Not doing anything yet. Just recently initialized.
  NEW {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return false;
    }
  },

  // Reading and mapping inputs (using stateless operators like filters, transforms).
  READING_INPUT {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == NEW;
    }
  },

  // Doing work that must be done *after* all inputs are read.
  // TODO(gianm): This doc is unclear; POST_READING really means we're doing a preshuffle and have determined what the
  //   output partition boundaries should be. It isn't valid in non-preshuffle contexts
  POST_READING {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT;
    }
  },

  // Done doing work and all results have been generated.
  RESULTS_READY {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT || priorPhase == POST_READING;
    }
  },

  // The worker outputs for this stage might have been cleaned up in the workers, and they cannot be used by
  // any other phase. "Metadata" for the stage such as counters are still available however
  FINISHED {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == RESULTS_READY;
    }
  },

  // Something went wrong.
  FAILED {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return true;
    }
  };

  public abstract boolean canTransitionFrom(ControllerStagePhase priorPhase);

  private static final Set<ControllerStagePhase> TERMINAL_PHASES = ImmutableSet.of(
      RESULTS_READY,
      FINISHED
  );

  /**
   * @return true if the phase indicates that the stage has completed its work and produced results successfully
   */
  public static boolean isSuccessfulTerminalPhase(final ControllerStagePhase phase)
  {
    return TERMINAL_PHASES.contains(phase);
  }

  private static final Set<ControllerStagePhase> POST_READING_PHASES = ImmutableSet.of(
      POST_READING,
      RESULTS_READY,
      FINISHED
  );

  /**
   * @return true if the phase indicates that the stage has consumed its inputs from the previous stages successfully
   */
  public static boolean isPostReadingPhase(final ControllerStagePhase phase)
  {
    return POST_READING_PHASES.contains(phase);
  }
}
