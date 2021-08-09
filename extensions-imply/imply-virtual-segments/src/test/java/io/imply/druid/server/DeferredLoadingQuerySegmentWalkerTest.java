/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.server;

import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DeferredLoadingQuerySegmentWalkerTest
{
  @Test
  public void testMinMaxSegmentsToQuery_singleIntervalSingleSegment()
  {
    Interval interval = Intervals.of("2021-08-04/2021-08-05");
    Set<SegmentDescriptor> descriptors = DeferredLoadingQuerySegmentWalker.segmentsToQueryForMetadata(
        Collections.singletonList(new SegmentDescriptor(interval, "v1", 0))
    );
    Assert.assertEquals(1, descriptors.size());
    SegmentDescriptor descriptor = descriptors.iterator().next();
    Assert.assertEquals(descriptor.toString(), interval, descriptor.getInterval());
    Assert.assertEquals(descriptor.toString(), "v1", descriptor.getVersion());
    Assert.assertEquals(descriptor.toString(), 0, descriptor.getPartitionNumber());
  }

  @Test
  public void testMinMaxSegmentsToQuery_singleIntervalMultipleSegments()
  {
    List<SegmentDescriptor> segmentDescriptors = new ArrayList<>();
    Interval interval = Intervals.of("2021-08-04/2021-08-05");
    for (int i = 0; i < 10; i++) {
      segmentDescriptors.add(new SegmentDescriptor(interval, "v1", i));
    }
    Set<SegmentDescriptor> descriptors = DeferredLoadingQuerySegmentWalker.segmentsToQueryForMetadata(segmentDescriptors);
    Assert.assertEquals(1, descriptors.size());
    SegmentDescriptor descriptor = descriptors.iterator().next();
    Assert.assertEquals(descriptor.toString(), interval, descriptor.getInterval());
    Assert.assertEquals(descriptor.toString(), "v1", descriptor.getVersion());
    Assert.assertTrue(descriptor.getPartitionNumber() < 10);
  }

  @Test
  public void testMinMaxSegmentsToQuery_multipleIntervalSingleSegment()
  {
    List<SegmentDescriptor> segmentDescriptors = new ArrayList<>();
    DateTime date = DateTimes.of("2021-08-04");
    for (int i = 0; i < 10; i++) {
      Interval interval = date.toLocalDate().toInterval(DateTimeZone.UTC);
      segmentDescriptors.add(new SegmentDescriptor(interval, "v1", 0));
      date = date.minusDays(1);
    }
    Set<SegmentDescriptor> descriptors = DeferredLoadingQuerySegmentWalker.segmentsToQueryForMetadata(segmentDescriptors);
    Set<SegmentDescriptor> expected = Sets.newHashSet(
        new SegmentDescriptor(Intervals.of("2021-08-04/2021-08-05"), "v1", 0),
        new SegmentDescriptor(Intervals.of("2021-07-26/2021-07-27"), "v1", 0)
    );
    Assert.assertEquals(expected, descriptors);
  }

  @Test
  public void testMinMaxSegmentsToQuery_multipleIntervalMultipleSegments()
  {
    List<SegmentDescriptor> segmentDescriptors = new ArrayList<>();
    DateTime date = DateTimes.of("2021-08-04");
    for (int i = 0; i < 10; i++) {
      Interval interval = date.toLocalDate().toInterval(DateTimeZone.UTC);
      segmentDescriptors.add(new SegmentDescriptor(interval, "v1", 0));
      segmentDescriptors.add(new SegmentDescriptor(interval, "v1", 1));
      date = date.minusDays(1);
    }
    Set<SegmentDescriptor> descriptors = DeferredLoadingQuerySegmentWalker.segmentsToQueryForMetadata(segmentDescriptors);
    Set<SegmentDescriptor> expected = Sets.newHashSet(
        new SegmentDescriptor(Intervals.of("2021-08-04/2021-08-05"), "v1", 0),
        new SegmentDescriptor(Intervals.of("2021-07-26/2021-07-27"), "v1", 0)
    );
    Assert.assertEquals(expected, descriptors);
  }
}
