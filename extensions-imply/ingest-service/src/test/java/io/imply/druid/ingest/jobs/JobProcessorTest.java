/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JobProcessorTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private OverlordClient overlordClient;
  private CoordinatorClient coordinatorClient;
  private IngestServiceMetadataStore metadataStore;
  private FileStore fileStore;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private DruidLeaderSelector leaderSelector;
  private DruidLeaderSelector.Listener leaderListener;

  private JobProcessor jobProcessor;

  @Before
  public void setup()
  {
    overlordClient = EasyMock.createMock(OverlordClient.class);
    coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    metadataStore = EasyMock.createMock(IngestServiceMetadataStore.class);
    fileStore = EasyMock.createMock(FileStore.class);
    scheduledExecutorFactory = EasyMock.createMock(ScheduledExecutorFactory.class);
    leaderSelector = EasyMock.createMock(DruidLeaderSelector.class);
    jobProcessor = new JobProcessor(
        overlordClient,
        coordinatorClient,
        metadataStore,
        fileStore,
        MAPPER,
        scheduledExecutorFactory,
        leaderSelector
    );
    leaderListener = EasyMock.createMock(DruidLeaderSelector.Listener.class);
  }

  @After
  public void teardown()
  {
    verifyAll();
  }

  @Test
  public void testLifecycleStartAndStop()
  {
    leaderSelector.registerListener(EasyMock.anyObject());
    EasyMock.expectLastCall();
    leaderSelector.unregisterListener();
    EasyMock.expectLastCall();
    replayAll();
    jobProcessor.start();
    jobProcessor.stop();
  }

  @Test
  public void testBecomeLeaderAndLoseLeader()
  {
    leaderSelector.registerListener(EasyMock.anyObject());
    EasyMock.expectLastCall();
    ScheduledExecutorService mockExec = EasyMock.createMock(ScheduledExecutorService.class);
    EasyMock.expect(scheduledExecutorFactory.create(1, "Ingest-Job-Exec--%d"))
            .andReturn(mockExec)
            .once();
    EasyMock.expect(mockExec.scheduleAtFixedRate(
        EasyMock.anyObject(),
        EasyMock.eq(1L),
        EasyMock.eq(1L),
        EasyMock.eq(TimeUnit.MINUTES)
    )).andReturn(EasyMock.createMock(ScheduledFuture.class)).once();
    EasyMock.expect(mockExec.shutdownNow()).andReturn(Collections.emptyList()).once();
    EasyMock.replay(mockExec);
    replayAll();
    jobProcessor.start();
    jobProcessor.becomeLeader();
    jobProcessor.stopBeingLeader();
    EasyMock.verify(mockExec);
  }

  void verifyAll()
  {
    EasyMock.verify(
        overlordClient,
        coordinatorClient,
        metadataStore,
        fileStore,
        scheduledExecutorFactory,
        leaderSelector,
        leaderListener
    );
  }

  void replayAll()
  {
    EasyMock.replay(
        overlordClient,
        coordinatorClient,
        metadataStore,
        fileStore,
        scheduledExecutorFactory,
        leaderSelector,
        leaderListener
    );
  }
}
