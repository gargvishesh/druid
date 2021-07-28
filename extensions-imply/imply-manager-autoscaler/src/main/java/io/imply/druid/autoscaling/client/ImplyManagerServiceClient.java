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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.imply.druid.autoscaling.ImplyManagerEnvironmentConfig;
import io.imply.druid.autoscaling.Instance;
import io.imply.druid.autoscaling.server.ImplyManagerServiceException;
import io.imply.druid.autoscaling.server.ListInstancesResponse;
import io.imply.druid.autoscaling.server.ProvisionInstancesRequest;
import io.imply.druid.autoscaling.server.ProvisionInstancesResponse;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Service Client for Imply Manager service public APIs
 */
public class ImplyManagerServiceClient
{
  private static final Logger LOGGER = new Logger(ImplyManagerServiceClient.class);
  private static final String HTTP_PROTOCOL = "http";
  private static final String HTTPS_PROTOCOL = "https";
  private static final String INSTANCES_API_PATH = "%s://%s/manager/v1/autoscaler/%s/instances";

  private final HttpClient client;
  private final ObjectMapper jsonMapper;

  public ImplyManagerServiceClient(HttpClient client, ObjectMapper jsonMapper)
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  public List<String> provisionInstances(
      ImplyManagerEnvironmentConfig implyConfig,
      @NotNull String workerVersion,
      int numToCreate
  ) throws IOException, ImplyManagerServiceException
  {
    ProvisionInstancesRequest requestBody = new ProvisionInstancesRequest(workerVersion, numToCreate);
    URL requestUrl = new URL(
        StringUtils.format(
            INSTANCES_API_PATH,
            implyConfig.isUseHttps() ? HTTPS_PROTOCOL : HTTP_PROTOCOL,
            implyConfig.getImplyManagerAddress(),
            implyConfig.getClusterId()
        )
    );
    final Request request = new Request(HttpMethod.POST, requestUrl);
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(requestBody));
    ProvisionInstancesResponse response = doRequest(request, ProvisionInstancesResponse.class);
    return response.getInstanceIds();
  }

  public void terminateInstance(
      ImplyManagerEnvironmentConfig implyConfig,
      String instanceIdToTerminate
  ) throws IOException, ImplyManagerServiceException
  {
    URL requestUrl = new URL(
        StringUtils.format(
            INSTANCES_API_PATH + "/%s",
            implyConfig.isUseHttps() ? HTTPS_PROTOCOL : HTTP_PROTOCOL,
            implyConfig.getImplyManagerAddress(),
            implyConfig.getClusterId(),
            instanceIdToTerminate
        )
    );

    final Request request = new Request(HttpMethod.DELETE, requestUrl);
    doRequest(request);
  }

  public List<Instance> listInstances(
      ImplyManagerEnvironmentConfig implyConfig
  ) throws IOException, ImplyManagerServiceException
  {
    URL requestUrl = new URL(
        StringUtils.format(
          INSTANCES_API_PATH,
          implyConfig.isUseHttps() ? HTTPS_PROTOCOL : HTTP_PROTOCOL,
          implyConfig.getImplyManagerAddress(),
          implyConfig.getClusterId()
        )
    );

    final Request request = new Request(HttpMethod.GET, requestUrl);
    ListInstancesResponse response = doRequest(request, ListInstancesResponse.class);
    List<Instance> instances = new ArrayList<>();
    for (ListInstancesResponse.Instance instanceResponse : response.getInstances()) {
      instances.add(new Instance(instanceResponse.getStatus(), instanceResponse.getIp(), instanceResponse.getId()));
    }
    return instances;
  }

  private <T> T doRequest(Request request, Class<T> clazz) throws IOException, ImplyManagerServiceException
  {
    InputStreamFullResponseHolder responseHolder = doRequest(request);
    return jsonMapper.readValue(responseHolder.getContent(), clazz);
  }

  @VisibleForTesting
  InputStreamFullResponseHolder doRequest(Request request) throws ImplyManagerServiceException
  {
    InputStreamFullResponseHolder responseHolder;
    try {
      responseHolder = client.go(
          request,
          new InputStreamFullResponseHandler()
      ).get();
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK) {
      throw new ImplyManagerServiceException(
          "Failed to talk to node at [%s]. Error code[%d], description[%s].",
          request.getUrl(),
          responseHolder.getStatus().getCode(),
          responseHolder.getStatus().getReasonPhrase()
      );
    }
    return responseHolder;
  }
}
