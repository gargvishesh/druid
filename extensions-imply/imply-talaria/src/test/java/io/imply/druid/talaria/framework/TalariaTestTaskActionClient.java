/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.talaria.indexing.error.InsertLockPreemptedFaultTest;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TalariaTestTaskActionClient implements TaskActionClient
{

  private static final String VERSION = "test";
  private final ObjectMapper mapper;
  private final ConcurrentHashMap<SegmentId, AtomicInteger> segmentIdPartitionIdMap = new ConcurrentHashMap<>();

  public TalariaTestTaskActionClient(ObjectMapper mapper)
  {
    this.mapper = mapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    if (taskAction instanceof SegmentAllocateAction) {
      SegmentAllocateAction segmentAllocateAction = (SegmentAllocateAction) taskAction;
      InsertLockPreemptedFaultTest.LockPreemptedHelper.throwIfPreempted();
      Granularity granularity = segmentAllocateAction.getPreferredSegmentGranularity();
      Interval interval;

      if (granularity instanceof PeriodGranularity) {
        PeriodGranularity periodGranularity = (PeriodGranularity) granularity;
        interval = new Interval(
            segmentAllocateAction.getTimestamp().toInstant(),
            periodGranularity.getPeriod()
        );
      } else {
        interval = Intervals.ETERNITY;
      }

      SegmentId segmentId = SegmentId.of(segmentAllocateAction.getDataSource(), interval, VERSION, 0);
      AtomicInteger newPartitionId = segmentIdPartitionIdMap.computeIfAbsent(segmentId, k -> new AtomicInteger(-1));

      return (RetType) new SegmentIdWithShardSpec(
          segmentAllocateAction.getDataSource(),
          interval,
          VERSION,
          segmentAllocateAction.getPartialShardSpec().complete(mapper, newPartitionId.addAndGet(1), 100)
      );
    } else if (taskAction instanceof LockListAction) {
      return (RetType) ImmutableList.of(new TimeChunkLock(
          TaskLockType.EXCLUSIVE,
          "group",
          "ds",
          Intervals.ETERNITY,
          VERSION,
          0
      ));
    } else if (taskAction instanceof RetrieveUsedSegmentsAction) {
      return (RetType) ImmutableSet.of();
    } else {
      return null;
    }
  }
}
