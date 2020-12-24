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
import com.google.common.collect.ImmutableMap;
import io.netty.util.CharsetUtil;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OverlordClientTest
{

  @Test
  public void testOverlordClientApi()
  {
    IndexingServiceClient indexingServiceClient = EasyMock.createMock(IndexingServiceClient.class);

    Interval interval = Intervals.ETERNITY;
    indexingServiceClient.killUnusedSegments("foo", "dataSource", interval);
    EasyMock.expectLastCall().once();
    EasyMock.replay(indexingServiceClient);

    // Just test one API since most of them just delegate...
    OverlordClient overlordClient = new OverlordClient(indexingServiceClient, null, null);
    overlordClient.killUnusedSegments("foo", "dataSource", interval);

    EasyMock.verify(indexingServiceClient);
  }

  @Test
  public void testGetTaskreport() throws IOException, InterruptedException
  {

    String taskId = "taskId";
    IndexingServiceClient indexingServiceClient = EasyMock.createMock(IndexingServiceClient.class);
    DruidLeaderClient druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);

    ObjectMapper objectMapper = EasyMock.createMock(ObjectMapper.class);

    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    ChannelBuffer buffer = ChannelBuffers.copiedBuffer(
        "content", CharsetUtil.UTF_8);
    EasyMock.expect(response.getStatus()).andReturn(HttpResponseStatus.OK).once();
    EasyMock.expect(response.getContent()).andReturn(buffer).once();
    EasyMock.replay(response);

    StringFullResponseHolder stringFullResponseHolder =
        new StringFullResponseHolder(HttpResponseStatus.ACCEPTED, response, StandardCharsets.UTF_8);

    Request request = EasyMock.createMock(Request.class);
    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format(
        "/druid/indexer/v1/task/%s/reports",
        StringUtils.urlEncode(taskId)
    ))).andReturn(request);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(stringFullResponseHolder).once();

    TaskReport taskReport = EasyMock.createMock(TaskReport.class);
    Map<String, TaskReport> taskreportMap = ImmutableMap.of("ingestionStatsAndErrors", taskReport);
    EasyMock.expect(objectMapper.readValue(
        stringFullResponseHolder.getContent(),
        OverlordClient.TASK_REPORT_TYPE_REFERENCE
    )).andReturn(taskreportMap).once();

    EasyMock.replay(indexingServiceClient, druidLeaderClient, request, taskReport, objectMapper);

    OverlordClient overlordClient = new OverlordClient(indexingServiceClient, druidLeaderClient, objectMapper);
    TaskReport result = overlordClient.getTaskReport(taskId);

    Assert.assertEquals(taskReport, result);

    EasyMock.verify(indexingServiceClient, druidLeaderClient, request, response);

  }
}
