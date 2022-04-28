/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Data Segment pusher which populates the {@link TalariaTestSegmentManager}
 */
public class TalariaTestDelegateDataSegmentPusher implements DataSegmentPusher
{
  private final DataSegmentPusher delegate;
  private final TalariaTestSegmentManager segmentManager;

  public TalariaTestDelegateDataSegmentPusher(
      DataSegmentPusher dataSegmentPusher,
      TalariaTestSegmentManager segmentManager
  )
  {
    delegate = dataSegmentPusher;
    this.segmentManager = segmentManager;
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return delegate.getPathForHadoop(dataSource);
  }

  @Override
  public String getPathForHadoop()
  {
    return delegate.getPathForHadoop();
  }

  @Override
  public DataSegment push(File file, DataSegment segment, boolean useUniquePath) throws IOException
  {
    final DataSegment dataSegment = delegate.push(file, segment, useUniquePath);
    segmentManager.addDataSegment(dataSegment);
    return dataSegment;
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    return delegate.makeLoadSpec(finalIndexZipFilePath);
  }
}
