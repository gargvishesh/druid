/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.row;

public class ConstantFrameRowPointer implements ReadableFrameRowPointer
{
  private final long start;
  private final long length;

  public ConstantFrameRowPointer(long start, long length)
  {
    this.start = start;
    this.length = length;
  }

  @Override
  public long position()
  {
    return start;
  }

  @Override
  public long length()
  {
    return length;
  }
}
