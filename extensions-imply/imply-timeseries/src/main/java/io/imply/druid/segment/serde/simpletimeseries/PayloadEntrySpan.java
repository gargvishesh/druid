/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

public class PayloadEntrySpan
{
  private final long start;
  private final int size;

  public PayloadEntrySpan(long start, int size)
  {
    this.start = start;
    this.size = size;
  }

  public long getStart()
  {
    return start;
  }

  public int getSize()
  {
    return size;
  }
}
