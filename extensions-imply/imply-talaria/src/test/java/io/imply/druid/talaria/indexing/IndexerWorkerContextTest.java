/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.Worker;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class IndexerWorkerContextTest
{

  private IndexerWorkerContext indexerWorkerContext = null;

  @Before
  public void setup()
  {
    final Injector injectorMock = Mockito.mock(Injector.class);
    Mockito.when(injectorMock.getInstance(SegmentCacheManagerFactory.class))
           .thenReturn(Mockito.mock(SegmentCacheManagerFactory.class));

    indexerWorkerContext = new IndexerWorkerContext(
        Mockito.mock(TaskToolbox.class),
        injectorMock,
        null,
        null,
        null
    );
  }

  @Test
  public void testLeaderCheckerRunnableExitsWhenEmptyStatus()
  {
    final ServiceLocator leaderLocatorMock = Mockito.mock(ServiceLocator.class);
    Mockito.when(leaderLocatorMock.locate())
           .thenReturn(Futures.immediateFuture(ServiceLocations.forLocations(Collections.emptySet())));

    final Worker workerMock = Mockito.mock(Worker.class);

    indexerWorkerContext.leaderCheckerRunnable(leaderLocatorMock, workerMock);
    Mockito.verify(leaderLocatorMock, Mockito.times(1)).locate();
    Mockito.verify(workerMock, Mockito.times(1)).leaderFailed();
  }

  @Test
  public void testLeaderCheckerRunnableExitsOnlyWhenClosedStatus()
  {
    final ServiceLocator leaderLocatorMock = Mockito.mock(ServiceLocator.class);
    Mockito.when(leaderLocatorMock.locate())
           .thenReturn(Futures.immediateFuture(ServiceLocations.forLocation(new ServiceLocation("h", 1, -1, "/"))))
           // Done to check the behavior of the runnable, the situation of exiting after success might not occur actually
           .thenReturn(Futures.immediateFuture(ServiceLocations.forLocation(new ServiceLocation("h", 1, -1, "/"))))
           .thenReturn(Futures.immediateFuture(ServiceLocations.closed()));

    final Worker workerMock = Mockito.mock(Worker.class);

    indexerWorkerContext.leaderCheckerRunnable(leaderLocatorMock, workerMock);
    Mockito.verify(leaderLocatorMock, Mockito.times(3)).locate();
    Mockito.verify(workerMock, Mockito.times(1)).leaderFailed();
  }
}
