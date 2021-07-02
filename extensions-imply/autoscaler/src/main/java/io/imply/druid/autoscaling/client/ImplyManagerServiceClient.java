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
import io.imply.druid.autoscaling.ImplyEnvironmentConfig;
import io.imply.druid.autoscaling.server.ImplyManagerServiceException;
import io.imply.druid.autoscaling.server.ListInstancesResponse;
import io.imply.druid.autoscaling.server.ProvisionInstancesRequest;
import io.imply.druid.autoscaling.server.ProvisionInstancesResponse;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Service Client for Imply Manager service public APIs
 */
public class ImplyManagerServiceClient
{
  private static final String INSTANCES_API_PATH = "https://%s/manager/v1/autoscaler/%s/instances";

  private final HttpClient client;
  private final ObjectMapper jsonMapper;

  public ImplyManagerServiceClient(HttpClient client, ObjectMapper jsonMapper)
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  public ProvisionInstancesResponse provisionInstances(
      ImplyEnvironmentConfig implyConfig,
      ProvisionInstancesRequest requestBody
  ) throws IOException, ImplyManagerServiceException
  {
    URL requestUrl = new URL(
        StringUtils.format(
            INSTANCES_API_PATH,
            implyConfig.getImplyAddress(),
            implyConfig.getClusterId()
        )
    );

    final Request request = new Request(HttpMethod.POST, requestUrl);
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(requestBody));
    return doRequest(request, ProvisionInstancesResponse.class);
  }

  public void terminateInstances(
      ImplyEnvironmentConfig implyConfig,
      String instanceIdToTerminate
  ) throws IOException, ImplyManagerServiceException
  {
    URL requestUrl = new URL(
        StringUtils.format(
            INSTANCES_API_PATH + "/%s",
            implyConfig.getImplyAddress(),
            implyConfig.getClusterId(),
            instanceIdToTerminate
        )
    );

    final Request request = new Request(HttpMethod.DELETE, requestUrl);
    doRequest(request);
  }

  public ListInstancesResponse listInstances(
      ImplyEnvironmentConfig implyConfig,
      List<Pair<String, String>> queryParams
  ) throws IOException, ImplyManagerServiceException
  {
    final String basePath = StringUtils.format(
        INSTANCES_API_PATH,
        implyConfig.getImplyAddress(),
        implyConfig.getClusterId()
    );
    final StringBuilder url = new StringBuilder();
    url.append(basePath);

    if (queryParams != null && !queryParams.isEmpty()) {
      String prefix = "?";
      for (Pair<String, String> param : queryParams) {
        if (param.rhs != null) {
          if (prefix != null) {
            url.append(prefix);
            prefix = null;
          } else {
            url.append("&");
          }
          url.append(StringUtils.urlEncode(param.lhs)).append("=").append(StringUtils.urlEncode(param.rhs));
        }
      }
    }

    URL requestUrl = new URL(url.toString());

    final Request request = new Request(HttpMethod.GET, requestUrl);
    return doRequest(request, ListInstancesResponse.class);
  }

  private <T> T doRequest(Request request, Class<T> clazz) throws IOException, ImplyManagerServiceException
  {
    InputStreamFullResponseHolder responseHolder = doRequest(request);
    return jsonMapper.readValue(responseHolder.getContent(), clazz);
  }

  private InputStreamFullResponseHolder doRequest(Request request) throws ImplyManagerServiceException
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
