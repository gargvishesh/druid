/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;

import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.timeline.DataSegment;

import javax.inject.Inject;

import java.io.File;

/**
 * This class does everything same as {@link SegmentLocalCacheManager} except {@link #isSegmentCached(DataSegment)}. This
 * is done so that {@link org.apache.druid.server.coordination.SegmentLoadDropHandler} will add the segments with info_dir
 * entry even if the segment is not actually present on the disk. If we don't override the behavior, restarts for
 * cold-historicals could be slow.
 */
public class VirtualSegmentCacheManager implements SegmentCacheManager
{
  private final SegmentLocalCacheManager physicalSegmentCacheManager;

  @Inject
  VirtualSegmentCacheManager(final SegmentLocalCacheManager physicalSegmentCacheManager)
  {
    this.physicalSegmentCacheManager = physicalSegmentCacheManager;
  }

  @Override
  public boolean isSegmentCached(DataSegment segment)
  {
    // Virtual segments are always cached by design.
    return true;
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    return physicalSegmentCacheManager.getSegmentFiles(segment);
  }

  @Override
  public boolean reserve(DataSegment segment)
  {
    return physicalSegmentCacheManager.reserve(segment);
  }

  @Override
  public boolean release(DataSegment segment)
  {
    return physicalSegmentCacheManager.reserve(segment);
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    physicalSegmentCacheManager.cleanup(segment);
  }
}
