/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.TalariaTasks;
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerContext;
import io.imply.druid.talaria.exec.WorkerImpl;
import io.imply.druid.talaria.util.TalariaContext;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.java.util.common.concurrent.Execs;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@JsonTypeName(TalariaWorkerTask.TYPE)
public class TalariaWorkerTask extends AbstractTask
{
  public static final String TYPE = "query_worker";

  private final String controllerTaskId;
  private final int workerNumber;

  // TODO(gianm): HACK HACK HACK
  @JacksonInject
  private Injector injector;

  private volatile Worker worker;
  private final boolean durableStorageEnabled;
  @Nullable
  private final ExecutorService remoteFetchExecutorService;

  @JsonCreator
  @VisibleForTesting
  public TalariaWorkerTask(
      @JsonProperty("controllerTaskId") final String controllerTaskId,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("workerNumber") final int workerNumber,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    super(
        TalariaTasks.workerTaskId(controllerTaskId, workerNumber),
        controllerTaskId,
        null,
        dataSource,
        context
    );
    this.controllerTaskId = controllerTaskId;
    this.workerNumber = workerNumber;
    durableStorageEnabled = TalariaContext.isDurableStorageEnabled(getContext());

    this.remoteFetchExecutorService = durableStorageEnabled
                                      ? Executors.newCachedThreadPool(Execs.makeThreadFactory(getId()
                                                                                              + "-remote-fetcher-%d"))
                                      : null;

  }

  @JsonProperty
  public String getControllerTaskId()
  {
    return controllerTaskId;
  }

  @JsonProperty
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @Nullable
  public ExecutorService getRemoteFetchExecutorService()
  {
    return remoteFetchExecutorService;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }


  @Override
  public boolean isReady(final TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    WorkerContext context = new IndexerWorkerContext(
        toolbox,
        injector,
        durableStorageEnabled,
        remoteFetchExecutorService
    );
    worker = new WorkerImpl(this, context);
    return worker.run();
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    if (worker != null) {
      worker.stopGracefully();
    }
    if (remoteFetchExecutorService != null) {
      // This is to make sure we donot leak connections.
      remoteFetchExecutorService.shutdownNow();
    }
  }
}
