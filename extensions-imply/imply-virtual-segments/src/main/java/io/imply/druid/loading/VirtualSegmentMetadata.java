/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;

import com.google.common.util.concurrent.SettableFuture;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegmentStateManagerImpl;

/**
 * Holder class for various metadata info about the virtual segment. This class is not thread-safe.
 */
public class VirtualSegmentMetadata
{
  private final VirtualReferenceCountingSegment virtualSegment;
  private volatile VirtualSegmentStateManagerImpl.Status status;
  private volatile SettableFuture<Void> downloadFuture;
  private volatile long queueStartTimeMillis = 0L;
  // Whether the segment is to removed from this server. This state is tracked separately through this
  // flag since a segment to be removed can also go through state transitions if the segment is in use already.
  private volatile boolean toBeRemoved = false;

  public VirtualSegmentMetadata(
      final VirtualReferenceCountingSegment virtualSegment,
      final VirtualSegmentStateManagerImpl.Status status
  )
  {
    this.status = status;
    this.virtualSegment = virtualSegment;
  }

  public VirtualReferenceCountingSegment getVirtualSegment()
  {
    return virtualSegment;
  }

  public VirtualSegmentStateManagerImpl.Status getStatus()
  {
    return status;
  }

  public void setStatus(VirtualSegmentStateManagerImpl.Status status)
  {
    this.status = status;
  }

  public SettableFuture<Void> getDownloadFuture()
  {
    return downloadFuture;
  }

  public void setDownloadFuture(SettableFuture<Void> future)
  {
    this.downloadFuture = future;
  }

  public void resetFuture()
  {
    downloadFuture = null;
  }

  public long getQueueStartTimeMillis()
  {
    return queueStartTimeMillis;
  }

  public void setQueueStartTimeMillis(long queueStartTimeMillis)
  {
    this.queueStartTimeMillis = queueStartTimeMillis;
  }

  public void markToBeRemoved()
  {
    this.toBeRemoved = true;
  }

  public void unmarkToBeRemoved()
  {
    this.toBeRemoved = false;
  }

  public boolean isToBeRemoved()
  {
    return this.toBeRemoved;
  }

}
