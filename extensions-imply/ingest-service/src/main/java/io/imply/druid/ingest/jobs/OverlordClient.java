/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OverlordClient implements IndexingServiceClient
{

  private IndexingServiceClient delegate;
  private DruidLeaderClient druidLeaderClient;
  private final ObjectMapper jsonMapper;

  static final TypeReference<Map<String, TaskReport>> TASK_REPORT_TYPE_REFERENCE;

  static {
    // this is mainly to make this unit testable...
    TASK_REPORT_TYPE_REFERENCE = new TypeReference<Map<String, TaskReport>>()
    {
    };
  }

  public OverlordClient(IndexingServiceClient delegate, DruidLeaderClient druidLeaderClient, ObjectMapper jsonMapper)
  {
    this.delegate = delegate;
    this.druidLeaderClient = druidLeaderClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void killUnusedSegments(String idPrefix, String dataSource, Interval interval)
  {
    delegate.killUnusedSegments(idPrefix, dataSource, interval);
  }

  @Override
  public int killPendingSegments(String dataSource, DateTime end)
  {
    return delegate.killPendingSegments(dataSource, end);
  }

  @Override
  public String compactSegments(
      String idPrefix,
      List<DataSegment> segments,
      int compactionTaskPriority,
      @Nullable ClientCompactionTaskQueryTuningConfig tuningConfig,
      @Nullable ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable Boolean dropExisting,
      @Nullable Map<String, Object> context
  )
  {
    return delegate.compactSegments(
        idPrefix,
        segments,
        compactionTaskPriority,
        tuningConfig,
        granularitySpec,
        dropExisting,
        context
    );
  }

  @Override
  public int getTotalWorkerCapacity()
  {
    return delegate.getTotalWorkerCapacity();
  }

  @Override
  public String runTask(String taskId, Object taskObject)
  {
    return delegate.runTask(taskId, taskObject);
  }

  @Override
  public String cancelTask(String taskId)
  {
    return delegate.cancelTask(taskId);
  }

  @Override
  public List<TaskStatusPlus> getActiveTasks()
  {
    return delegate.getActiveTasks();
  }

  @Override
  public TaskStatusResponse getTaskStatus(String taskId)
  {
    return delegate.getTaskStatus(taskId);
  }

  @Override
  public Map<String, TaskStatus> getTaskStatuses(Set<String> taskIds) throws InterruptedException
  {
    return delegate.getTaskStatuses(taskIds);
  }

  @Nullable
  @Override
  public TaskStatusPlus getLastCompleteTask()
  {
    return delegate.getLastCompleteTask();
  }

  @Nullable
  @Override
  public TaskPayloadResponse getTaskPayload(String taskId)
  {
    return delegate.getTaskPayload(taskId);
  }

  @Override
  public SamplerResponse sample(SamplerSpec samplerSpec)
  {
    return delegate.sample(samplerSpec);
  }

  @Nullable
  public TaskReport getTaskReport(String taskId)
  {
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format(
              "/druid/indexer/v1/task/%s/reports",
              StringUtils.urlEncode(taskId)
          ))
      );
      if (responseHolder.getResponse().getStatus().getCode() != HttpResponseStatus.OK.getCode()) {
        return null;
      }
      Map<String, TaskReport> taskReport = jsonMapper.readValue(
          responseHolder.getContent(), TASK_REPORT_TYPE_REFERENCE);
      return taskReport.get("ingestionStatsAndErrors");
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
