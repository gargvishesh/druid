/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.storage.StorageConnector;
import io.imply.druid.talaria.exec.LeaderClient;
import io.imply.druid.talaria.exec.TalariaDataSegmentProvider;
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.exec.WorkerContext;
import io.imply.druid.talaria.exec.WorkerMemoryParameters;
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.guice.Talaria;
import io.imply.druid.talaria.indexing.error.DurableStorageConfigurationFault;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.rpc.DruidServiceClientFactory;
import io.imply.druid.talaria.rpc.ServiceLocations;
import io.imply.druid.talaria.rpc.ServiceLocator;
import io.imply.druid.talaria.rpc.StandardRetryPolicy;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import io.imply.druid.talaria.rpc.indexing.SpecificTaskRetryPolicy;
import io.imply.druid.talaria.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class IndexerWorkerContext implements WorkerContext
{
  private static final Logger log = new Logger(IndexerWorkerContext.class);
  private static final long FREQUENCY_CHECK_MILLIS = 1000;
  private static final long FREQUENCY_CHECK_JITTER = 30;

  private final TaskToolbox toolbox;
  private final Injector injector;
  private final IndexIO indexIO;
  private final TalariaDataSegmentProvider dataSegmentProvider;
  private final DruidServiceClientFactory clientFactory;
  private final boolean durableStorageEnabled;

  @GuardedBy("this")
  private OverlordServiceClient overlordClient;

  @GuardedBy("this")
  private LeaderClient leaderClient;

  @GuardedBy("this")
  private ServiceLocator leaderLocator;

  @Nullable
  private final ExecutorService remoteFetchExecutorService;

  public IndexerWorkerContext(
      TaskToolbox toolbox,
      Injector injector,
      boolean durableStorageEnabled,
      @Nullable
      ExecutorService remoteFetchExecutorService
  )
  {
    this.toolbox = toolbox;
    this.injector = injector;
    this.durableStorageEnabled = durableStorageEnabled;

    final SegmentCacheManager segmentCacheManager =
        injector.getInstance(SegmentCacheManagerFactory.class)
                .manufacturate(new File(toolbox.getIndexingTmpDir(), "segment-fetch"));
    this.indexIO = injector.getInstance(IndexIO.class);
    this.dataSegmentProvider = new TalariaDataSegmentProvider(segmentCacheManager, this.indexIO);
    this.clientFactory = injector.getInstance(Key.get(DruidServiceClientFactory.class, EscalatedGlobal.class));
    this.remoteFetchExecutorService = remoteFetchExecutorService;
  }

  // Note: NOT part of the interface! Not available in the Talaria server.
  public TaskToolbox toolbox()
  {
    return toolbox;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return toolbox.getJsonMapper();
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void registerWorker(Worker worker, Closer closer)
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, worker);
    toolbox.getChatHandlerProvider().register(worker.id(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(worker.id()));
    closer.register(() -> {
      synchronized (this) {
        if (leaderLocator != null) {
          leaderLocator.close();
        }
      }
    });

    // Register the periodic leader checker
    final ExecutorService periodicLeaderCheckerExec = Execs.singleThreaded("leader-status-checker-%s");
    closer.register(periodicLeaderCheckerExec::shutdownNow);
    final ServiceLocator leaderLocator = makeLeaderLocator(worker.task().getControllerTaskId());
    periodicLeaderCheckerExec.submit(() -> leaderCheckerRunnable(leaderLocator, worker));
  }

  @VisibleForTesting
  void leaderCheckerRunnable(final ServiceLocator leaderLocator, final Worker worker)
  {
    while (true) {
      // Add some randomness to the frequency of the loop to avoid requests from simultaneously spun up tasks bunching
      // up and stagger them randomly
      long sleepTimeMillis = FREQUENCY_CHECK_MILLIS + ThreadLocalRandom.current().nextLong(
          -FREQUENCY_CHECK_JITTER,
          2 * FREQUENCY_CHECK_JITTER
      );
      final ServiceLocations leaderLocations;
      try {
        leaderLocations = leaderLocator.locate().get();
      }
      catch (Throwable e) {
        // Service locator exceptions are not recoverable.
        log.noStackTrace().warn(
            e,
            "Periodic fetch of leader location encountered an exception. Worker task [%s] will exit.",
            worker.id()
        );
        worker.leaderFailed();
        break;
      }

      if (leaderLocations.isClosed() || leaderLocations.getLocations().isEmpty()) {
        log.warn(
            "Periodic fetch of leader location returned [%s]. Worker task [%s] will exit.",
            leaderLocations,
            worker.id()
        );
        worker.leaderFailed();
        break;
      }

      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ignored) {
        // Do nothing: an interrupt means we were shut down. Status checker should exit quietly.
      }
    }
  }

  @Override
  public File tempDir()
  {
    return toolbox.getIndexingTmpDir();
  }

  @Override
  public synchronized LeaderClient makeLeaderClient(String leaderId)
  {
    if (leaderClient == null) {
      final ServiceLocator locator = makeLeaderLocator(leaderId);

      leaderClient = new IndexerLeaderClient(
          clientFactory.makeClient(leaderId, locator, new SpecificTaskRetryPolicy(leaderId)),
          jsonMapper(),
          locator
      );
    }
    return leaderClient;
  }

  @Override
  public WorkerClient makeWorkerClient(String leaderId, String workerId)
  {
    // Ignore workerId parameter. The workerId is passed into each method of WorkerClient individually.
    if (durableStorageEnabled) {

      final StorageConnector storageConnector;
      try {
        storageConnector = injector().getInstance(Key.get(StorageConnector.class, Talaria.class));
      }
      catch (ConfigurationException configurationException) {
        throw new TalariaException(new DurableStorageConfigurationFault(configurationException.getMessage()));
      }

      return new IndexerWorkerClient(
          leaderId,
          clientFactory,
          makeOverlordClient(),
          jsonMapper(),
          storageConnector,
          durableStorageEnabled,
          remoteFetchExecutorService
      );
    } else {
      return new IndexerWorkerClient(
          leaderId,
          clientFactory,
          makeOverlordClient(),
          jsonMapper(),
          null,
          durableStorageEnabled,
          null
      );
    }
  }

  @Override
  public FrameContext frameContext(QueryDefinition queryDef, int stageNumber)
  {
    final int numWorkersInJvm;

    // TODO(gianm): hack alert!! need to know max # of workers in jvm for memory allocations
    if (toolbox.getAppenderatorsManager() instanceof UnifiedIndexerAppenderatorsManager) {
      // CliIndexer
      numWorkersInJvm = injector.getInstance(WorkerConfig.class).getCapacity();
    } else {
      // CliPeon
      numWorkersInJvm = 1;
    }

    return new IndexerFrameContext(
        this,
        indexIO,
        dataSegmentProvider,
        WorkerMemoryParameters.compute(
            Runtime.getRuntime().maxMemory(),
            numWorkersInJvm,
            processorBouncer().getMaxCount(),
            queryDef.getStageDefinition(stageNumber)
                    .getInputStageIds()
                    .stream()
                    .mapToInt(stageId -> queryDef.getStageDefinition(stageId).getMaxWorkerCount())
                    .sum()
        )
    );
  }

  @Override
  public int threadCount()
  {
    return processorBouncer().getMaxCount();
  }

  @Override
  public DruidNode selfNode()
  {
    return injector.getInstance(Key.get(DruidNode.class, Self.class));
  }

  @Override
  public Bouncer processorBouncer()
  {
    return injector.getInstance(Bouncer.class);
  }

  private synchronized OverlordServiceClient makeOverlordClient()
  {
    if (overlordClient == null) {
      overlordClient = injector.getInstance(OverlordServiceClient.class)
                               .withRetryPolicy(StandardRetryPolicy.unlimited());
    }
    return overlordClient;
  }

  private synchronized ServiceLocator makeLeaderLocator(final String leaderId)
  {
    if (leaderLocator == null) {
      leaderLocator = new SpecificTaskServiceLocator(leaderId, makeOverlordClient());
    }

    return leaderLocator;
  }
}
