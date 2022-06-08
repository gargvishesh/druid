/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write.columnar;

import org.apache.datasketches.memory.WritableMemory;

import java.io.Closeable;

public interface FrameColumnWriter extends Closeable
{
  boolean addSelection();

  void undo();

  long size();

  long writeTo(WritableMemory memory, long position);

  @Override
  void close();
}
