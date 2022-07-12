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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderContext;
import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.exec.WorkerImpl;
import io.imply.druid.talaria.exec.WorkerManagerClient;
import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TalariaTestLeaderContext implements LeaderContext
{
  private static final Logger log = new Logger(TalariaTestLeaderContext.class);
  private final TaskActionClient taskActionClient;
  private final Map<String, Worker> inMemoryWorkers = new HashMap<>();
  private final ConcurrentMap<String, TaskStatus> statusMap = new ConcurrentHashMap<>();
  private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Execs.singleThreaded(
      "Talaria-test-leader-client"));
  private final CoordinatorClient coordinatorClient;
  private final DruidNode node = new DruidNode(
      "leader",
      "localhost",
      true,
      8080,
      8081,
      true,
      false
  );
  private final Injector injector;
  private final ObjectMapper mapper;

  private Leader leader;
  private Map<String, TaskReport> report = null;

  public TalariaTestLeaderContext(ObjectMapper mapper, Injector injector, TaskActionClient taskActionClient)
  {
    this.mapper = mapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.fetchUsedSegmentsInDataSourceForIntervals(
                     ArgumentMatchers.anyString(),
                     ArgumentMatchers.anyList()
                 )
    ).thenAnswer(invocation ->
                     (injector.getInstance(SpecificSegmentsQuerySegmentWalker.class)
                              .getSegments()
                              .stream()
                              .filter(dataSegment -> dataSegment.getDataSource().equals(invocation.getArguments()[0]))
                              .collect(Collectors.toList())
                     )
    );
  }

  WorkerManagerClient workerManagerClient = new WorkerManagerClient()
  {
    @Override
    public String run(String leaderId, TalariaWorkerTask task)
    {
      if (leader == null) {
        throw new ISE("Leader needs to be set using the register method");
      }
      Worker worker = new WorkerImpl(
          task,
          new TalariaTestWorkerContext(inMemoryWorkers, leader, mapper, injector)
      );
      inMemoryWorkers.put(task.getId(), worker);
      statusMap.put(task.getId(), TaskStatus.running(task.getId()));

      ListenableFuture<TaskStatus> future = executor.submit(worker::run);

      Futures.addCallback(future, new FutureCallback<TaskStatus>()
      {
        @Override
        public void onSuccess(@Nullable TaskStatus result)
        {
          statusMap.put(task.getId(), result);
        }

        @Override
        public void onFailure(Throwable t)
        {
          log.error(t, "error running worker task %s", task.getId());
          statusMap.put(task.getId(), TaskStatus.failure(task.getId(), t.getMessage()));
        }
      });

      return task.getId();
    }

    @Override
    public Map<String, TaskStatus> statuses(Set<String> taskIds)
    {
      Map<String, TaskStatus> result = new HashMap<>();
      for (String taskId : taskIds) {
        TaskStatus taskStatus = statusMap.get(taskId);
        if (taskStatus != null) {

          if (taskStatus.getStatusCode().equals(TaskState.RUNNING) && !inMemoryWorkers.containsKey(taskId)) {
            result.put(taskId, new TaskStatus(taskId, TaskState.FAILED, 0, null, null));
          } else {
            result.put(
                taskId,
                new TaskStatus(
                    taskStatus.getId(),
                    taskStatus.getStatusCode(),
                    taskStatus.getDuration(),
                    null,
                    null
                )
            );
          }
        }
      }
      return result;
    }

    @Override
    public TaskLocation location(String workerId)
    {
      final TaskStatus status = statusMap.get(workerId);
      if (status != null && status.getStatusCode().equals(TaskState.RUNNING) && inMemoryWorkers.containsKey(workerId)) {
        return TaskLocation.create("host-" + workerId, 1, -1);
      } else {
        return TaskLocation.unknown();
      }
    }

    @Override
    public void cancel(String workerId)
    {
      final Worker worker = inMemoryWorkers.remove(workerId);
      if (worker != null) {
        worker.stopGracefully();
      }
    }

    @Override
    public void close()
    {
      //do nothing
    }
  };

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
  public DruidNode selfNode()
  {
    return node;
  }

  @Override
  public CoordinatorClient coordinatorClient()
  {
    return coordinatorClient;
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    return taskActionClient;
  }

  @Override
  public WorkerManagerClient workerManager()
  {
    return workerManagerClient;
  }

  @Override
  public void registerLeader(Leader leader, Closer closer)
  {
    this.leader = leader;
  }

  @Override
  public WorkerClient taskClientFor(Leader leader)
  {
    return new TalariaTestWorkerClient(inMemoryWorkers);
  }

  @Override
  public void writeReports(String taskId, Map<String, TaskReport> taskReport)
  {
    if (leader != null && leader.id().equals(taskId)) {
      report = taskReport;
    }
  }

  public Map<String, TaskReport> getAllReports()
  {
    return report;
  }
}
