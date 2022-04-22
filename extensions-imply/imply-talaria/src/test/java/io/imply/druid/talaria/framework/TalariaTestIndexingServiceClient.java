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
import io.imply.druid.talaria.exec.LeaderImpl;
import io.imply.druid.talaria.indexing.TalariaControllerTask;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskDimensionsSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TalariaTestIndexingServiceClient implements IndexingServiceClient
{
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final TaskActionClient taskActionClient;
  private Map<String, Leader> inMemmoryLeaders = new HashMap<>();
  private Map<String, Map<String, TaskReport>> reports = new HashMap<>();
  private Map<String, TalariaQuerySpec> talariaQuerySpec = new HashMap<>();

  public TalariaTestIndexingServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient
  )
  {
    this.objectMapper = objectMapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
  }

  @Override
  public void killUnusedSegments(String idPrefix, String dataSource, Interval interval)
  {
    throw new UOE("Unsupported call in test client");
  }

  @Override
  public int killPendingSegments(String dataSource, DateTime end)
  {
    throw new UOE("Unsupported call in test client");
  }

  @Override
  public String compactSegments(
      String idPrefix,
      List<DataSegment> segments,
      int compactionTaskPriority,
      @Nullable ClientCompactionTaskQueryTuningConfig tuningConfig,
      @Nullable ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable ClientCompactionTaskDimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      @Nullable ClientCompactionTaskTransformSpec transformSpec,
      @Nullable Boolean dropExisting,
      @Nullable Map<String, Object> context
  )
  {
    throw new UOE("Unsupported call in test client");
  }

  @Override
  public int getTotalWorkerCapacity()
  {
    return 1;
  }

  @Override
  public int getTotalWorkerCapacityWithAutoScale()
  {
    return 1;
  }

  @Override
  public String runTask(String taskId, Object taskObject)
  {
    org.apache.druid.indexer.TaskStatus status;
    LeaderImpl leader = null;
    TalariaTestLeaderContext talariaTestLeaderContext = null;
    try {
      talariaTestLeaderContext = new TalariaTestLeaderContext(objectMapper, injector, taskActionClient);

      TalariaControllerTask cTask = objectMapper.convertValue(taskObject, TalariaControllerTask.class);
      talariaQuerySpec.put(cTask.getId(), cTask.getQuerySpec());

      leader = new LeaderImpl(
          cTask,
          talariaTestLeaderContext
      );

      inMemmoryLeaders.put(cTask.getId(), leader);
      status = leader.run();


    }
    catch (Exception exception) {
      throw new ISE(exception.getCause(), "Unable to run");
    }
    finally {
      if (leader != null && talariaTestLeaderContext != null) {
        reports.put(leader.id(), talariaTestLeaderContext.getAllReports());
      }
    }

    return status != null ? status.getId() : null;
  }

  @Override
  public String cancelTask(String taskId)
  {
    inMemmoryLeaders.get(taskId).stopGracefully();
    return taskId;
  }

  @Override
  public List<TaskStatusPlus> getActiveTasks()
  {
    throw new UOE("Unsupported call in test client");
  }

  @Override
  public TaskStatusResponse getTaskStatus(String taskId)
  {
    throw new UOE("Unsupported call in test client");
  }

  @Override
  public Map<String, TaskStatus> getTaskStatuses(Set<String> taskIds)
  {
    throw new UOE("Unsupported call in test client");
  }

  @Nullable
  @Override
  public TaskStatusPlus getLastCompleteTask()
  {
    throw new UOE("Unsupported call in test client");

  }

  @Nullable
  @Override
  public TaskPayloadResponse getTaskPayload(String taskId)
  {
    throw new UOE("Unsupported call in test client");
  }

  @Nullable
  @Override
  public Map<String, Object> getTaskReport(String taskId)
  {
    return (Map) inMemmoryLeaders.get(taskId).liveReports();
  }

  @Override
  public Map<String, List<Interval>> getLockedIntervals(Map<String, Integer> minTaskPriority)
  {
    throw new UOE("Unsupported call in test client");
  }

  @Override
  public SamplerResponse sample(SamplerSpec samplerSpec)
  {
    throw new UOE("Unsupported call in test client");
  }

  // hooks to pull stuff out for testing
  @Nullable
  Map<String, TaskReport> getReportForTask(String id)
  {
    return reports.get(id);
  }

  @Nullable
  TalariaQuerySpec getQuerySpecForTask(String id)
  {
    return talariaQuerySpec.get(id);
  }

}
