/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegment;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;

public class TestData
{
  private static final int SEGMENT_SIZE = 100;

  public static VirtualReferenceCountingSegment buildVirtualSegment()
  {
    return buildVirtualSegment(1);
  }

  public static VirtualReferenceCountingSegment buildVirtualSegment(int partitionId)
  {
    DataSegment dataSegment = buildDataSegment(partitionId);
    VirtualSegment segment = new VirtualSegment(dataSegment, false, null);
    return VirtualReferenceCountingSegment.wrapRootGenerationSegment(segment);
  }

  public static DataSegment buildDataSegment(int partitionId)
  {
    SegmentId segmentId = SegmentId.dummy("data-source", partitionId);
    return new DataSegment(
        segmentId,
        null,
        null,
        null,
        new NumberedShardSpec(partitionId, 10),
        null,
        0,
        SEGMENT_SIZE
    );
  }
}
