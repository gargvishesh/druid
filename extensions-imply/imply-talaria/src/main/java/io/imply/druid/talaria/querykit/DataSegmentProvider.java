/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import io.imply.druid.talaria.counters.ChannelCounters;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;

public interface DataSegmentProvider
{
  ResourceHolder<Segment> fetchSegment(
      SegmentId segmentId,
      ChannelCounters channelCounters
  );
}
