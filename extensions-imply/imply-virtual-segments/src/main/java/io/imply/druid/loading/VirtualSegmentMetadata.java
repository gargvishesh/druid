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
  private VirtualSegmentStateManagerImpl.Status status;
  private SettableFuture<Void> downloadFuture;

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

}
