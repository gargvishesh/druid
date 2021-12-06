/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import io.imply.druid.talaria.frame.read.Frame;

public class FrameWithPartition
{
  public static final int NO_PARTITION = -1;

  private final Frame frame;
  private final int partition;

  public FrameWithPartition(Frame frame, int partition)
  {
    this.frame = frame;
    this.partition = partition;
  }

  public Frame frame()
  {
    return frame;
  }

  public int partition()
  {
    return partition;
  }
}
