/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.List;

/**
 * Utility functions for working with {@link InputSpec}.
 */
public class InputSpecs
{
  private InputSpecs()
  {
    // No instantiation.
  }

  public static IntSet getStageNumbers(final List<InputSpec> specs)
  {
    final IntSet retVal = new IntRBTreeSet();

    for (final InputSpec spec : specs) {
      if (spec instanceof StageInputSpec) {
        retVal.add(((StageInputSpec) spec).getStageNumber());
      }
    }

    return retVal;
  }
}
