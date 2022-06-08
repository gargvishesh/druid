/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;

/**
 * Wraps a {@link Frame} and provides ways to compare rows of this frame to various other things.
 *
 * Not thread-safe.
 */
public interface FrameComparisonWidget
{
  /**
   * Returns the {@link ClusterByKey} corresponding to a particular row. The returned key is a copy that does
   * not reference memory of the underlying {@link Frame}.
   */
  ClusterByKey readKey(int row);

  /**
   * Compare a specific row of this frame to the provided key. The key must have been created with sortColumns
   * that match the ones used to create this widget, or else results are undefined.
   */
  int compare(int row, ClusterByKey key);

  /**
   * Compare a specific row of this frame to a specific row of another frame. The other frame must have the same
   * signature, or else results are undefined. The other frame may be the same object as this frame; for example,
   * this is used by {@link io.imply.druid.talaria.frame.write.FrameSort} to sort frames in-place.
   */
  int compare(int row, FrameComparisonWidget otherWidget, int otherRow);
}
