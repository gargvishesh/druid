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
import io.imply.druid.TestData;
import io.imply.druid.loading.FIFOSegmentReplacementStrategy;
import io.imply.druid.loading.VirtualSegmentMetadata;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;

public class VirtualSegmentStateTransitionTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private VirtualSegmentStateManagerImpl virtualSegmentHolder;

  @Before
  public void setup()
  {
    virtualSegmentHolder = new VirtualSegmentStateManagerImpl(new FIFOSegmentReplacementStrategy());
  }

  @Test
  public void testReady()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    Assert.assertNull(virtualSegmentHolder.registerIfAbsent(segment));
    VirtualReferenceCountingSegment otherSegment = TestData.buildVirtualSegment();
    VirtualReferenceCountingSegment current = virtualSegmentHolder.registerIfAbsent(otherSegment);
    Assert.assertSame(segment, current);
    Assert.assertSame(segment, virtualSegmentHolder.get(segment.getId()));
    assertStatus(VirtualSegmentStateManagerImpl.Status.READY, segment.getId());
  }

  @Test
  public void testRemoveToReady()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    Assert.assertNull(virtualSegmentHolder.registerIfAbsent(segment));
    virtualSegmentHolder.remove(segment.getId());
    Assert.assertEquals(0, virtualSegmentHolder.size());
    VirtualReferenceCountingSegment newSegment = TestData.buildVirtualSegment();
    Assert.assertNull(virtualSegmentHolder.registerIfAbsent(newSegment));
    Assert.assertSame(newSegment, virtualSegmentHolder.get(newSegment.getId()));
    Assert.assertEquals(1, virtualSegmentHolder.size());
  }

  @Test
  public void testReadyToQueue()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    ListenableFuture<Void> future = virtualSegmentHolder.queue(segment);
    assertStatus(VirtualSegmentStateManagerImpl.Status.QUEUED, segment.getId());
    Assert.assertFalse(future.isDone());
    Assert.assertFalse(future.isCancelled());
    Assert.assertSame(segment, virtualSegmentHolder.get(segment.getId()));
  }

  @Test
  public void testQueueToQueue()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    ListenableFuture<Void> future = virtualSegmentHolder.queue(segment);
    Assert.assertSame(future, virtualSegmentHolder.queue(segment));
    Assert.assertSame(segment, virtualSegmentHolder.get(segment.getId()));
    assertStatus(VirtualSegmentStateManagerImpl.Status.QUEUED, segment.getId());
  }

  @Test
  public void testDownloadToQueue()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    Assert.assertSame(segment, virtualSegmentHolder.get(segment.getId()));
    ListenableFuture<Void> future = virtualSegmentHolder.queue(segment);
    Assert.assertTrue(future.isDone());
    assertStatus(VirtualSegmentStateManagerImpl.Status.DOWNLOADED, segment.getId());
  }

  @Test
  public void testEvictToQueue()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    virtualSegmentHolder.evict(segment);
    Assert.assertSame(segment, virtualSegmentHolder.get(segment.getId()));
    ListenableFuture<Void> future = virtualSegmentHolder.queue(segment);
    Assert.assertFalse(future.isDone());
    assertStatus(VirtualSegmentStateManagerImpl.Status.QUEUED, segment.getId());
  }

  @Test
  public void testReadyToDownload()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    Assert.assertSame(segment, virtualSegmentHolder.get(segment.getId()));
    assertStatus(VirtualSegmentStateManagerImpl.Status.DOWNLOADED, segment.getId());
    Assert.assertTrue(virtualSegmentHolder.getMetadata(segment.getId()).getDownloadFuture().isDone());
  }

  @Test
  public void testQueueToDownload()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    ListenableFuture<Void> future = virtualSegmentHolder.queue(segment);
    Assert.assertFalse(future.isDone());
    virtualSegmentHolder.downloaded(segment);
    Assert.assertTrue(future.isDone());
    assertStatus(VirtualSegmentStateManagerImpl.Status.DOWNLOADED, segment.getId());
  }

  @Test
  public void testDownloadToDownload()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    virtualSegmentHolder.downloaded(segment);
    assertStatus(VirtualSegmentStateManagerImpl.Status.DOWNLOADED, segment.getId());
    Assert.assertTrue(virtualSegmentHolder.getMetadata(segment.getId()).getDownloadFuture().isDone());
  }

  @Test
  public void testEvictToDownload()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    virtualSegmentHolder.evict(segment);
    virtualSegmentHolder.downloaded(segment);
    assertStatus(VirtualSegmentStateManagerImpl.Status.DOWNLOADED, segment.getId());
    Assert.assertTrue(virtualSegmentHolder.getMetadata(segment.getId()).getDownloadFuture().isDone());
  }

  @Test
  public void testDownloadToEvict()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    virtualSegmentHolder.evict(segment);
    assertStatus(VirtualSegmentStateManagerImpl.Status.READY, segment.getId());
    Assert.assertNull(virtualSegmentHolder.getMetadata(segment.getId()).getDownloadFuture());
  }

  @Test
  public void testQueueToEvict()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.queue(segment);

    expectedException.expect(ISE.class);
    expectedException.expectMessage("being asked to evict but is not marked downloaded. Current state [QUEUED]");
    virtualSegmentHolder.evict(segment);
  }

  @Test
  public void testQueueToRemove() throws ExecutionException, InterruptedException
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    SegmentId segmentId = segment.getId();
    virtualSegmentHolder.registerIfAbsent(segment);
    ListenableFuture<Void> future = virtualSegmentHolder.queue(segment);
    virtualSegmentHolder.remove(segment.getId());
    Assert.assertNull(virtualSegmentHolder.getMetadata(segmentId));
    Assert.assertTrue(future.isDone());
    expectedException.expect(ExecutionException.class);
    expectedException.expectMessage("Segment was removed");
    future.get();
  }

  @Test
  public void testDownloadToRemove()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    SegmentId segmentId = segment.getId();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    virtualSegmentHolder.remove(segment.getId());
    Assert.assertNull(virtualSegmentHolder.getMetadata(segmentId));
  }

  @Test
  public void testEvictToRemove()
  {
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment();
    SegmentId segmentId = segment.getId();
    virtualSegmentHolder.registerIfAbsent(segment);
    virtualSegmentHolder.downloaded(segment);
    virtualSegmentHolder.evict(segment);
    virtualSegmentHolder.remove(segment.getId());
    Assert.assertNull(virtualSegmentHolder.getMetadata(segmentId));
  }

 

  private void assertStatus(VirtualSegmentStateManagerImpl.Status status, SegmentId segmentId)
  {
    VirtualSegmentMetadata metadata = virtualSegmentHolder.getMetadata(segmentId);
    Assert.assertEquals(status, metadata.getStatus());
  }
}
