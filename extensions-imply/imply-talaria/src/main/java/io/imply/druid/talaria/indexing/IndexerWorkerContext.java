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
import com.google.common.base.Optional;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.talaria.exec.LeaderClient;
import io.imply.druid.talaria.exec.LeaderStatusClient;
import io.imply.druid.talaria.exec.TalariaDataSegmentProvider;
import io.imply.druid.talaria.exec.TalariaTaskClient;
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.exec.WorkerContext;
import io.imply.druid.talaria.exec.WorkerImpl;
import io.imply.druid.talaria.exec.WorkerMemoryParameters;
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.kernel.QueryDefinition;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.server.DruidNode;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class IndexerWorkerContext implements WorkerContext
{
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final String taskId;
  private final IndexIO indexIO;
  private final TalariaDataSegmentProvider dataSegmentProvider;
  private TalariaTaskClient taskClient;

  private static final long FREQUENCY_CHECK_MILLIS = 1000;
  private static final long FREQUENCY_CHECK_JITTER = 30;

  private static final Logger log = new Logger(IndexerWorkerContext.class);

  public IndexerWorkerContext(TaskToolbox toolbox, Injector injector, String taskId)
  {
    this.toolbox = toolbox;
    this.injector = injector;
    this.taskId = taskId;

    final SegmentCacheManager segmentCacheManager =
        injector.getInstance(SegmentCacheManagerFactory.class)
                .manufacturate(new File(toolbox.getIndexingTmpDir(), "segment-fetch"));
    this.indexIO = injector.getInstance(IndexIO.class);
    this.dataSegmentProvider = new TalariaDataSegmentProvider(segmentCacheManager, this.indexIO);
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
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, (WorkerImpl) worker);
    toolbox.getChatHandlerProvider().register(worker.id(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(worker.id()));

    // Register the periodic leader checker
    final ExecutorService periodicLeaderCheckerExec = Execs.singleThreaded("leader-status-checker-%s");
    closer.register(periodicLeaderCheckerExec::shutdownNow);

    LeaderStatusClient leaderStatusClient = new IndexerLeaderStatusClient(
        toolbox.getIndexingServiceClient(),
        worker.task().getControllerTaskId()
    );
    closer.register(leaderStatusClient::close);
    periodicLeaderCheckerExec.submit(() -> leaderCheckerRunnable(leaderStatusClient, worker));
  }

  @VisibleForTesting
  void leaderCheckerRunnable(final LeaderStatusClient leaderStatusClient, final Worker worker)
  {
    while (true) {
      // Add some randomness to the frequency of the loop to avoid requests from simultaneously spun up tasks bunching
      // up and stagger them randomly
      long sleepTimeMillis = FREQUENCY_CHECK_MILLIS + ThreadLocalRandom.current().nextLong(
          -FREQUENCY_CHECK_JITTER,
          2 * FREQUENCY_CHECK_JITTER
      );
      boolean leaderFailed = false;
      Optional<TaskStatus> taskStatusOptional;
      try {
        taskStatusOptional = leaderStatusClient.status();
      }
      catch (Exception e) {
        log.warn("Error occurred while fetching the status of the leader client. Retrying...");
        continue;
      }

      if (!taskStatusOptional.isPresent()) {
        log.debug("Period fetch of the status of leader task didn't return anything");
        leaderFailed = true;
      } else {
        TaskStatus taskStatus = taskStatusOptional.get();
        log.debug("Periodic fetch of the status of leader task returned [%s]", taskStatus.getStatusCode());
        if (taskStatus.isFailure()) {
          leaderFailed = true;
        }
      }

      if (leaderFailed) {
        log.warn("Invalid leader status. Calling leaderFailed() on the worker");
        worker.leaderFailed();
        break;
      }

      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ignored) {
        log.error("Leader status checker interrupted while sleeping");
        break;
      }
    }
  }

  @Override
  public File tempDir()
  {
    return toolbox.getIndexingTmpDir();
  }

  @Override
  public LeaderClient makeLeaderClient(String id)
  {
    // "id" is ignored because the leader id is discovered as part of individual method calls.
    return makeTaskClient();
  }

  @Override
  public WorkerClient makeWorkerClient(String id)
  {
    // "id" is ignored because the worker id is provided as part of individual method calls.
    return makeTaskClient();
  }

  /**
   * In the indexer, the leader and worker clients are the same thing.
   */
  @Override
  public synchronized TalariaTaskClient makeTaskClient()
  {
    if (taskClient == null) {
      taskClient = new TalariaIndexerTaskClient(
          injector.getInstance(Key.get(HttpClient.class, EscalatedGlobal.class)),
          jsonMapper(),
          new ClientBasedTaskInfoProvider(toolbox.getIndexingServiceClient()),
          taskId
      );
    }
    return taskClient;
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
}
