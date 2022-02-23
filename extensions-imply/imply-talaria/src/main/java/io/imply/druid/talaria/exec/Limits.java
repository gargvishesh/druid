/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

public class Limits
{
  /**
   * Maximum number of columns that can appear in a frame signature.
   *
   * Somewhat less than {@link WorkerMemoryParameters#STANDARD_FRAME_SIZE} divided by typical minimum column size:
   * {@link io.imply.druid.talaria.frame.AppendableMemory#DEFAULT_INITIAL_ALLOCATION_SIZE}.
   */
  public static final int MAX_FRAME_COLUMNS = 2000;

  /**
   * Maximum number of workers that can be used in a stage, regardless of available memory.
   */
  public static final int MAX_WORKERS = 1000;
}
