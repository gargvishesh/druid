/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.autoscaling.ImplyManagerEnvironmentConfig;
import io.imply.druid.autoscaling.Instance;
import io.imply.druid.autoscaling.server.ImplyManagerServiceException;
import io.imply.druid.autoscaling.server.ListInstancesResponse;
import io.imply.druid.autoscaling.server.ProvisionInstancesRequest;
import io.imply.druid.autoscaling.server.ProvisionInstancesResponse;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class ImplyManagerServiceClientTest
{
  private static final String IMPLY_MANAGER_ADDRESS = "www.implymanager.io";
  private static final String CLUSTER_ID = "123-123";

  @Mock
  private HttpClient mockHttpClient;

  private ObjectMapper objectMapper = new ObjectMapper();
  private ImplyManagerEnvironmentConfig implyManagerEnvironmentConfig;
  private ImplyManagerServiceClient implyManagerServiceClient;

  @Before
  public void setUp()
  {
    implyManagerServiceClient = new ImplyManagerServiceClient(
        mockHttpClient,
        objectMapper
    );
    implyManagerEnvironmentConfig = new ImplyManagerEnvironmentConfig(IMPLY_MANAGER_ADDRESS, CLUSTER_ID);
  }

  @Test
  public void testProvisionInstances() throws Exception
  {
    String workerVersion = "999";
    int numberToCreate = 1;

    String createdNodeId = "new-node-id";
    ProvisionInstancesResponse response = new ProvisionInstancesResponse(
        ImmutableList.of(
            new ProvisionInstancesResponse.ProvisionInstanceResponse(workerVersion, ImmutableList.of(createdNodeId))
        )
    );
    InputStream inputStream = new ByteArrayInputStream(objectMapper.writeValueAsBytes(response));
    InputStreamFullResponseHolder responseHolder = Mockito.mock(InputStreamFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(inputStream);
    Mockito.when(responseHolder.getStatus()).thenReturn(HttpResponseStatus.OK);

    ListenableFuture listenableFuture = Mockito.mock(ListenableFuture.class);
    Mockito.when(listenableFuture.get()).thenReturn(responseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class), ArgumentMatchers.any(InputStreamFullResponseHandler.class))).thenReturn(listenableFuture);

    List<String> actual = implyManagerServiceClient.provisionInstances(implyManagerEnvironmentConfig, workerVersion, numberToCreate);

    ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(mockHttpClient).go(requestArgumentCaptor.capture(), ArgumentMatchers.any(HttpResponseHandler.class));
    Mockito.verifyNoMoreInteractions(mockHttpClient);

    // Verify captured request
    Request request = requestArgumentCaptor.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(IMPLY_MANAGER_ADDRESS, request.getUrl().getHost());
    Assert.assertEquals(StringUtils.format("/manager/v1/autoscaler/%s/instances", CLUSTER_ID), request.getUrl().getPath());
    ProvisionInstancesRequest actualRequestBody = objectMapper.readValue(request.getContent().array(), ProvisionInstancesRequest.class);
    Assert.assertNotNull(actualRequestBody);
    Assert.assertEquals(1, actualRequestBody.getInstances().size());
    Assert.assertEquals(numberToCreate, actualRequestBody.getInstances().get(0).getNumToCreate());
    Assert.assertEquals(workerVersion, actualRequestBody.getInstances().get(0).getVersion());

    // Verify response
    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(createdNodeId, actual.get(0));
  }

  @Test
  public void testTerminateInstances() throws Exception
  {
    String instanceIdToTerminate = "999";

    InputStreamFullResponseHolder responseHolder = Mockito.mock(InputStreamFullResponseHolder.class);
    Mockito.when(responseHolder.getStatus()).thenReturn(HttpResponseStatus.OK);

    ListenableFuture listenableFuture = Mockito.mock(ListenableFuture.class);
    Mockito.when(listenableFuture.get()).thenReturn(responseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class), ArgumentMatchers.any(InputStreamFullResponseHandler.class))).thenReturn(listenableFuture);

    implyManagerServiceClient.terminateInstance(implyManagerEnvironmentConfig, instanceIdToTerminate);

    ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(mockHttpClient).go(requestArgumentCaptor.capture(), ArgumentMatchers.any(HttpResponseHandler.class));
    Mockito.verifyNoMoreInteractions(mockHttpClient);

    // Verify captured request
    Request request = requestArgumentCaptor.getValue();
    Assert.assertEquals(HttpMethod.DELETE, request.getMethod());
    Assert.assertEquals(IMPLY_MANAGER_ADDRESS, request.getUrl().getHost());
    Assert.assertEquals(StringUtils.format("/manager/v1/autoscaler/%s/instances/" + instanceIdToTerminate, CLUSTER_ID), request.getUrl().getPath());
    Assert.assertNull(request.getContent());
  }

  @Test
  public void testListInstances() throws Exception
  {
    String instanceStatus1 = "RUNNING";
    String instanceIp1 = "1.1.1.1";
    String instanceId1 = "id1";
    String instanceStatus2 = "TERMINATING";
    String instanceIp2 = "2.2.2.2";
    String instanceId2 = "id2";
    ListInstancesResponse response = new ListInstancesResponse(
        ImmutableList.of(
            new ListInstancesResponse.Instance(instanceStatus1, instanceIp1, instanceId1),
            new ListInstancesResponse.Instance(instanceStatus2, instanceIp2, instanceId2)
        )
    );
    InputStream inputStream = new ByteArrayInputStream(objectMapper.writeValueAsBytes(response));
    InputStreamFullResponseHolder responseHolder = Mockito.mock(InputStreamFullResponseHolder.class);
    Mockito.when(responseHolder.getContent()).thenReturn(inputStream);
    Mockito.when(responseHolder.getStatus()).thenReturn(HttpResponseStatus.OK);

    ListenableFuture listenableFuture = Mockito.mock(ListenableFuture.class);
    Mockito.when(listenableFuture.get()).thenReturn(responseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class), ArgumentMatchers.any(InputStreamFullResponseHandler.class))).thenReturn(listenableFuture);

    List<Instance> actual = implyManagerServiceClient.listInstances(implyManagerEnvironmentConfig);

    ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(mockHttpClient).go(requestArgumentCaptor.capture(), ArgumentMatchers.any(HttpResponseHandler.class));
    Mockito.verifyNoMoreInteractions(mockHttpClient);

    // Verify captured request
    Request request = requestArgumentCaptor.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(IMPLY_MANAGER_ADDRESS, request.getUrl().getHost());
    Assert.assertEquals(StringUtils.format("/manager/v1/autoscaler/%s/instances", CLUSTER_ID), request.getUrl().getPath());
    Assert.assertNull(request.getContent());

    // Verify response
    Assert.assertNotNull(actual);
    Assert.assertEquals(2, actual.size());
    Assert.assertTrue(actual.contains(new Instance(instanceStatus1, instanceIp1, instanceId1)));
    Assert.assertTrue(actual.contains(new Instance(instanceStatus2, instanceIp2, instanceId2)));
  }

  @Test(expected = ImplyManagerServiceException.class)
  public void testDoRequestErrorFromServer() throws Exception
  {
    Request mockRequest = Mockito.mock(Request.class);

    InputStreamFullResponseHolder responseHolder = Mockito.mock(InputStreamFullResponseHolder.class);
    Mockito.when(responseHolder.getStatus()).thenReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    ListenableFuture listenableFuture = Mockito.mock(ListenableFuture.class);
    Mockito.when(listenableFuture.get()).thenReturn(responseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class), ArgumentMatchers.any(InputStreamFullResponseHandler.class))).thenReturn(listenableFuture);

    implyManagerServiceClient.doRequest(mockRequest);
  }
}
