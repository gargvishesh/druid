/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestUsedSegmentChecker implements UsedSegmentChecker
{
  private final List<DataSegment> pushedSegments;

  public TestUsedSegmentChecker(List<DataSegment> pushedSegments)
  {
    this.pushedSegments = pushedSegments;
  }

  @Override
  public Set<DataSegment> findUsedSegments(Set<SegmentIdWithShardSpec> identifiers)
  {
    final SegmentTimeline timeline = new SegmentTimeline();
    timeline.addSegments(pushedSegments.iterator());

    final Set<DataSegment> retVal = new HashSet<>();
    for (SegmentIdWithShardSpec identifier : identifiers) {
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.lookup(identifier.getInterval())) {
        for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
          if (identifiers.contains(SegmentIdWithShardSpec.fromDataSegment(chunk.getObject()))) {
            retVal.add(chunk.getObject());
          }
        }
      }
    }

    return retVal;
  }
}
