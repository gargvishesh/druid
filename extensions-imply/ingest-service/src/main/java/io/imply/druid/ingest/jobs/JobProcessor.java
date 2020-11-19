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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.duty.StartScheduledJobsDuty;
import io.imply.druid.ingest.jobs.duty.UpdateRunningJobsStatusDuty;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@ManageLifecycle
public class JobProcessor
{
  private static final Logger LOG = new Logger(JobProcessor.class);

  private final ReentrantLock lock = new ReentrantLock(true);

  private final JobProcessingContext jobProcessingContext;

  private final List<JobProcessorDuty> duties;
  private final ScheduledExecutorService exec;

  @Inject
  public JobProcessor(
      IndexingServiceClient indexingServiceClient,
      CoordinatorClient coordinatorClient,
      IngestServiceMetadataStore metadataStore,
      FileStore fileStore,
      @Json ObjectMapper jsonMapper,
      ScheduledExecutorFactory scheduledExecutorFactory
  )
  {
    this.jobProcessingContext = new JobProcessingContext(
        indexingServiceClient,
        coordinatorClient,
        metadataStore,
        fileStore,
        jsonMapper
    );
    this.duties = ImmutableList.of(
        new UpdateRunningJobsStatusDuty(jobProcessingContext),
        new StartScheduledJobsDuty(jobProcessingContext)
    );
    this.exec = scheduledExecutorFactory.create(1, "Ingest-Job-Exec--%d");
  }

  @LifecycleStart
  public void start()
  {
    // todo: this should be tied to leadership election instead of lifecycle start
    exec.scheduleAtFixedRate(
        () -> {
          // anything else?
          duties.forEach(JobProcessorDuty::run);
          // todo: maybe emit some metrics?
        },
        // todo: this stuff should be from configuration.
        1,
        1,
        TimeUnit.MINUTES
    );
  }

  @LifecycleStop
  public void stop()
  {
    // todo: something something leadership election
    exec.shutdownNow();
  }

  public JobProcessingContext getJobProcessorContext()
  {
    return jobProcessingContext;
  }
}
