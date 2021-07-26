/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;


import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;

/**
 * SegmentReplacementStrategy is used to decide
 *  - which segment should be downloaded first from a pool of pending segments
 *  - which segment should be evicted first from a pool of downloaded segments
 */
public interface SegmentReplacementStrategy
{
  /**
   * Returns the id of the next segment id to download. This method will return a different segment on each call. Null
   * is returned if there are no more segments to download.
   */
  @Nullable
  SegmentId nextProcess();

  /**
   * Returns the id of the next segment that can be evicted. This method can be called multiple times and return the same
   * segment. Null is returned if there are no segments to evict.
   */
  @Nullable
  SegmentId nextEvict();

  /**
   * Register that the given segment was downloaded
   */
  void downloaded(SegmentId segment, VirtualSegmentMetadata metadata);

  /**
   * Queue a segment for download in future. Same segment can be requested to queue multiple times and implementation
   * should handle it.
   */
  void queue(SegmentId segment, VirtualSegmentMetadata metadata);

  /**
   * Remove a segment completely.
   */
  void remove(SegmentId segment);

}
