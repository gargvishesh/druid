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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.ingest.IngestionService;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.duty.StartScheduledJobsDuty;
import io.imply.druid.ingest.jobs.duty.UpdateRunningJobsStatusDuty;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class JobProcessor
{
  private static final Logger LOG = new Logger(JobProcessor.class);

  private final Object lock = new Object();

  private final ScheduledExecutorFactory scheduledExecutorFactory;
  private final JobProcessingContext jobProcessingContext;

  private final List<JobProcessorDuty> duties;

  private final DruidLeaderSelector leaderSelector;

  private volatile boolean started = false;
  private ScheduledExecutorService exec;

  @Inject
  public JobProcessor(
      OverlordClient indexingServiceClient,
      CoordinatorClient coordinatorClient,
      IngestServiceMetadataStore metadataStore,
      FileStore fileStore,
      @Json ObjectMapper jsonMapper,
      ScheduledExecutorFactory scheduledExecutorFactory,
      @IngestionService final DruidLeaderSelector leaderSelector
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
    this.leaderSelector = leaderSelector;
    this.scheduledExecutorFactory = scheduledExecutorFactory;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      started = true;

      leaderSelector.registerListener(
          new DruidLeaderSelector.Listener()
          {
            @Override
            public void becomeLeader()
            {
              JobProcessor.this.becomeLeader();
            }

            @Override
            public void stopBeingLeader()
            {
              JobProcessor.this.stopBeingLeader();
            }
          }
      );
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }
      leaderSelector.unregisterListener();

      started = false;
    }
  }

  @VisibleForTesting
  public void becomeLeader()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      LOG.info("I am the leader!");

      exec = scheduledExecutorFactory.create(1, "Ingest-Job-Exec--%d");
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
  }

  @VisibleForTesting
  public void stopBeingLeader()
  {
    synchronized (lock) {
      LOG.info("I lost leadership!\n"
               + "⢀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⣠⣤⣶⣶\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⢰⣿⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⣀⣀⣾⣿⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⡏⠉⠛⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⠀⠀⠀⠈⠛⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠛⠉⠁⠀⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣧⡀⠀⠀⠀⠀⠙⠿⠿⠿⠻⠿⠿⠟⠿⠛⠉⠀⠀⠀⠀⠀⣸⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⣿⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠠⣴⣿⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⢰⣹⡆⠀⠀⠀⠀⠀⠀⣭⣷⠀⠀⠀⠸⣿⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⠃⠀⠀⠈⠉⠀⠀⠤⠄⠀⠀⠀⠉⠁⠀⠀⠀⠀⢿⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⢾⣿⣷⠀⠀⠀⠀⡠⠤⢄⠀⠀⠀⠠⣿⣿⣷⠀⢸⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⡀⠉⠀⠀⠀⠀⠀⢄⠀⢀⠀⠀⠀⠀⠉⠉⠁⠀⠀⣿⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⠀⠈⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⣿⣿\n"
               + "⣿⣿⣿⣿⣿⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿");
      if (exec != null) {
        exec.shutdownNow();
        exec = null;
      }
    }
  }
}
