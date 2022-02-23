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
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.talaria.exec.LeaderClient;
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
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.server.DruidNode;
import org.joda.time.Duration;

import java.io.File;

public class IndexerWorkerContext implements WorkerContext
{
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final String taskId;
  private final IndexIO indexIO;
  private final TalariaDataSegmentProvider dataSegmentProvider;
  private TalariaTaskClient taskClient;

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
          Duration.standardMinutes(5),
          taskId,
          999999 /* TODO(gianm): hardcoded to a very large number until we build proper fault tolerance */
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
