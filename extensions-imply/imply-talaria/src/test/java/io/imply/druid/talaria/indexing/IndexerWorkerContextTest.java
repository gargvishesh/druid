/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.base.Optional;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.LeaderStatusClient;
import io.imply.druid.talaria.exec.Worker;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class IndexerWorkerContextTest
{

  private IndexerWorkerContext indexerWorkerContext = null;
  private static final String TASK_ID = "dummy-id";

  @Before
  public void setup()
  {
    final Injector injectorMock = Mockito.mock(Injector.class);
    Mockito.when(injectorMock.getInstance(SegmentCacheManagerFactory.class))
           .thenReturn(Mockito.mock(SegmentCacheManagerFactory.class));

    indexerWorkerContext = new IndexerWorkerContext(
        Mockito.mock(TaskToolbox.class),
        injectorMock,
        TASK_ID
    );
  }

  @Test
  public void testLeaderCheckerRunnableExitsWhenEmptyStatus()
  {
    final LeaderStatusClient leaderStatusClientMock = Mockito.mock(LeaderStatusClient.class);
    Mockito.when(leaderStatusClientMock.status()).thenReturn(Optional.absent());

    final Worker workerMock = Mockito.mock(Worker.class);

    indexerWorkerContext.leaderCheckerRunnable(leaderStatusClientMock, workerMock);
    Mockito.verify(leaderStatusClientMock, Mockito.times(1)).status();
    Mockito.verify(workerMock, Mockito.times(1)).leaderFailed();
  }

  @Test
  public void testLeaderCheckerRunnableExitsOnlyWhenFailedStatus()
  {
    final LeaderStatusClient leaderStatusClientMock = Mockito.mock(LeaderStatusClient.class);
    Mockito.when(leaderStatusClientMock.status())
           .thenReturn(Optional.of(TaskStatus.running(TASK_ID)))
           // Done to check the behavior of the runnable, the situation of failing after success might not occur actually
           .thenReturn(Optional.of(TaskStatus.success(TASK_ID)))
           .thenReturn(Optional.of(TaskStatus.failure(TASK_ID, "dummy-error")));

    final Worker workerMock = Mockito.mock(Worker.class);

    indexerWorkerContext.leaderCheckerRunnable(leaderStatusClientMock, workerMock);
    Mockito.verify(leaderStatusClientMock, Mockito.times(3)).status();
    Mockito.verify(workerMock, Mockito.times(1)).leaderFailed();
  }
}
