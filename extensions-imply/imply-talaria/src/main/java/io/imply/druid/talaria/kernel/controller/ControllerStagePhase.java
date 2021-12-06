/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

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
  RESULTS_COMPLETE {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT || priorPhase == POST_READING;
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
}
