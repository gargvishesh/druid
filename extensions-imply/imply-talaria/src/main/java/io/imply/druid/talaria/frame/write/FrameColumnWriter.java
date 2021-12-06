/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public interface FrameColumnWriter extends Closeable
{
  boolean addSelection();

  void undo();

  /**
   * Compares two rows that have already been written. Used by {@link FrameWriter#sort} for sorting frames prior to
   * writing them out.
   *
   * Behavior of this comparison must be consistent with the same-typed
   * {@link io.imply.druid.talaria.frame.cluster.ClusterByColumnWidget}.
   */
  int compare(int row1, int row2);

  long size();

  void writeTo(WritableByteChannel channel) throws IOException;

  @Override
  void close();
}
