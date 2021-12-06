/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

public class MemoryLimits
{
  /**
   * Percent of memory allocated to {@link org.apache.druid.segment.incremental.IncrementalIndex} during
   * the add-and-persist phase of segment generation.
   */
  public static final double APPENDERATOR_INDEX_MEMORY_PERCENT = 0.2;

  /**
   * Percent of memory allocated to reading columns during the merge phase of segment generation.
   */
  public static final double APPENDERATOR_MERGE_MEMORY_PERCENT = 0.3;

  /**
   * Percent of memory allocated to frames.
   */
  public static final double FRAME_MEMORY_PERCENT = 0.6;

  /**
   * Size of frames.
   *
   * TODO(gianm): would be useful to use smaller frames for final supersorter output, in order to ensure next stage
   *   can read prior stage outputs using 1-level merge
   */
  public static final int FRAME_SIZE = 8_000_000;

  /**
   * Maximum number of columns that can appear in a frame signature.
   *
   * Somewhat less than {@link #FRAME_SIZE} divided by typical minimum column size, which is somewhere between
   * {@link io.imply.druid.talaria.frame.AppendableMemory#INITIAL_ALLOCATION_SIZE} (long, float, double) and 3x that same
   * constant (string).
   */
  public static final int FRAME_MAX_COLUMNS = 2_000;

  private MemoryLimits()
  {
    // No instantiation.
  }
}
