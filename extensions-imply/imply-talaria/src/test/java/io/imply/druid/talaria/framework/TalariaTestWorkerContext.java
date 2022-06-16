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
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderClient;
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.exec.WorkerContext;
import io.imply.druid.talaria.exec.WorkerMemoryParameters;
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.indexing.IndexerFrameContext;
import io.imply.druid.talaria.indexing.IndexerWorkerContext;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.security.AuthTestUtils;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class TalariaTestWorkerContext implements WorkerContext
{
  private final Leader leader;
  private final ObjectMapper mapper;
  private final Injector injector;
  private final Map<String, Worker> inMemoryWorkers;
  private final File file = FileUtils.createTempDir();
  private final ExecutorService remoteExecutorService;

  public TalariaTestWorkerContext(
      Map<String, Worker> inMemoryWorkers,
      Leader leader,
      ObjectMapper mapper,
      Injector injector,
      ExecutorService remoteExecutorService
  )
  {
    this.inMemoryWorkers = inMemoryWorkers;
    this.leader = leader;
    this.mapper = mapper;
    this.injector = injector;
    this.remoteExecutorService = remoteExecutorService;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return mapper;
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void registerWorker(Worker worker, Closer closer)
  {

  }

  @Override
  public LeaderClient makeLeaderClient(String leaderId)
  {
    return new TalariaTestLeaderClient(leader);
  }

  @Override
  public WorkerClient makeWorkerClient()
  {
    return new TalariaTestWorkerClient(inMemoryWorkers);
  }

  @Override
  public File tempDir()
  {
    return file;
  }

  @Override
  public FrameContext frameContext(QueryDefinition queryDef, int stageNumber)
  {
    IndexIO indexIO = new IndexIO(
        mapper,
        () -> 0
    );
    IndexMergerV9 indexMerger = new IndexMergerV9(
        mapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    final TaskReportFileWriter reportFileWriter = new TaskReportFileWriter()
    {
      @Override
      public void write(String taskId, Map<String, TaskReport> reports)
      {

      }

      @Override
      public void setObjectMapper(ObjectMapper objectMapper)
      {

      }
    };

    return new IndexerFrameContext(
        new IndexerWorkerContext(
            new TaskToolbox.Builder()
                .segmentPusher(injector.getInstance(DataSegmentPusher.class))
                .segmentAnnouncer(injector.getInstance(DataSegmentAnnouncer.class))
                .jsonMapper(mapper)
                .taskWorkDir(tempDir())
                .indexIO(indexIO)
                .indexMergerV9(indexMerger)
                .taskReportFileWriter(reportFileWriter)
                .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
                .chatHandlerProvider(new NoopChatHandlerProvider())
                .rowIngestionMetersFactory(NoopRowIngestionMeters::new)
                .build(),
            injector
        ),
        indexIO,
        injector.getInstance(DataSegmentProvider.class),
        WorkerMemoryParameters.compute(
            WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
            2,
            10,
            2
        )
    );
  }

  @Override
  public int threadCount()
  {
    return 1;
  }

  @Override
  public DruidNode selfNode()
  {
    return new DruidNode("test", "123", true, 8080, 8081, true, false);
  }

  @Override
  public Bouncer processorBouncer()
  {
    return injector.getInstance(Bouncer.class);
  }
}
