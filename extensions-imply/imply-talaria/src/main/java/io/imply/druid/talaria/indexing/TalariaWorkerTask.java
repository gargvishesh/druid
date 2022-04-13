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
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerContext;
import io.imply.druid.talaria.exec.WorkerImpl;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;

import javax.annotation.Nullable;

import java.util.Map;

@JsonTypeName(TalariaWorkerTask.TYPE)
public class TalariaWorkerTask extends AbstractTask
{
  public static final String TYPE = "talaria1";

  private final String controllerTaskId;

  // TODO(gianm): HACK HACK HACK
  @JacksonInject
  private Injector injector;

  private volatile Worker worker;

  @JsonCreator
  @VisibleForTesting
  public TalariaWorkerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("controllerTaskId") final String controllerTaskId,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, TYPE, dataSource),
        groupId,
        null,
        dataSource,
        context
    );

    this.controllerTaskId = controllerTaskId;
  }

  @JsonProperty
  public String getControllerTaskId()
  {
    return controllerTaskId;
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
    WorkerContext context = new IndexerWorkerContext(toolbox, injector, getId());
    worker = new WorkerImpl(this, context);
    return worker.run();
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    if (worker != null) {
      worker.stopGracefully();
    }
  }
}
