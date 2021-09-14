/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment;

import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.loading.VirtualSegmentLoader;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;

/**
 * <p>
 * This class keeps track of state management of virtual segments. State transitions for a given segment must be atomic
 * in nature. An error will be thrown if state is inconsistent for a segment and an operation is performed for this
 * segment.
 * From a caller's pespective, the operations should look like they were done in a serial order for each segment.
 * Consumers of this class such as {@link VirtualSegmentLoader} rely on this assumption to build their own
 * thread-safe logic.
 * <p>
 * With the serialization guarantees, callers can safely queue and download segments concurrently.
 * <p>
 * These states are useful in determining which segment can be downloaded or can be evicted or are already downloaded.
 * </p>
 *
 * <p>
 * froThis class also decides which virtual segment to download next or which virtual segment to evict first to make space. The
 * implementation needs to be thread safe and must support a transaction like capabilities (ACID) except the durability.
 * </p>
 */
public interface VirtualSegmentStateManager
{
  /**
   * Adds a segment if its not already added.
   *
   * @param segment - Virtual segment to be added
   * @return Previously added segment or null if there was no entry present before
   */
  @Nullable
  VirtualReferenceCountingSegment registerIfAbsent(VirtualReferenceCountingSegment segment);

  /**
   * Re-queues a segment for download. There is no new reference acquired since re-queue is not done for a particular
   * query.
   */
  void requeue(VirtualReferenceCountingSegment segment);

  /**
   * Queues a segment for download and acquire a reference together. The acquired reference is cleared when
   * closer passed as the parameter is closed.
   * @param segment - Segment to queue
   * @param closer - Closer, which the reference release will be registered with.
   * @return {@link ListenableFuture} which is completed successfully when segment is downloaded or with an error
   * if the download fails. Queuing a segment without registering it first will result in an error.
   */
  ListenableFuture<Void> queue(VirtualReferenceCountingSegment segment, Closer closer);

  /**
   * Cancels the download of the segment. If the segment is already queued for
   * download, it is removed from the queue. Otherwise, this has no effect.
   *
   * @return true, if the download has been cancelled or is already complete.
   * false if the download is still required
   */
  boolean cancelDownload(VirtualReferenceCountingSegment segment);

  /**
   * @return The segment that is eligible to be downloaded next.
   */
  @Nullable
  VirtualReferenceCountingSegment toDownload();

  /**
   * Marks a segment as downloaded.
   */
  void downloaded(VirtualReferenceCountingSegment segment);

  /**
   * Register a download error for the segment. Segments can be rescheduled for download if {@param recoverable}
   * is set to true.
   */
  void downloadFailed(VirtualReferenceCountingSegment segment, boolean recoverable);

  /**
   * Evicts a segment. Eviction only deletes the content from disk but does not de-register the segment since it can be
   * re-used.
   */
  void evict(VirtualReferenceCountingSegment segment);

  /**
   * De-register the segment. Any state associated with the segment is removed. {@link #beginRemove} must be called before
   * calling this function.
   * @return whether the segment was actually removed or not.
   */
  boolean finishRemove(SegmentId segmentId);

  /**
   * Soft removal of a segment. Any state associated with the segment is still intact but new queries might get rejected
   */
  void beginRemove(SegmentId segmentId);

  /**
   * Gets the Virtual segment entry from {@link SegmentId} .
   */
  @Nullable
  VirtualReferenceCountingSegment get(SegmentId segmentId);

  /**
   * @return Returns the segment that is eligible for eviction.
   */
  @Nullable
  VirtualReferenceCountingSegment toEvict();
}
