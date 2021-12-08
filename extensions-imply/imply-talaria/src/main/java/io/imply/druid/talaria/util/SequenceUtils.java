/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import org.apache.druid.java.util.common.guava.Sequence;

import java.util.function.Consumer;

/**
 * Sequence-related utility functions that would make sense in {@link Sequence} if this were not an extension.
 */
public class SequenceUtils
{
  /**
   * Executes "action" for each element of "sequence".
   */
  public static <T> void forEach(Sequence<T> sequence, Consumer<? super T> action)
  {
    sequence.accumulate(
        null,
        (accumulated, in) -> {
          action.accept(in);
          return null;
        }
    );
  }
}
