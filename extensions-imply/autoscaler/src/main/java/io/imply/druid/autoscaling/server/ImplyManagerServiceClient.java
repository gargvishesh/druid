/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.server;

import com.sun.org.apache.regexp.internal.RE;
import io.imply.druid.autoscaling.ImplyEnvironmentConfig;
import sun.net.www.http.HttpClient;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;

/**
 * Service Client for Imply Manager service public APIs
 */
public class ImplyManagerServiceClient
{
  private static final String PROVISION_PATH = "instances/createInstances";
  private static final String TERMINATE_PATH = "instances/deleteInstances";
  private static final String LIST_PATH = "instances/listInstances";

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
  ) throws IOException
  {
    URL requestUrl = new URL(
        StringUtils.format(
            "https://%s/v1/druid/%s/%s",
            implyConfig.getImplyAddress(),
            implyConfig.getClusterId(),
            PROVISION_PATH
        )
    );

    final Request request = Request(HttpMethod.POST, requestUrl);
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(requestBody));
    return doRequest(request, ProvisionInstancesResponse.class);
  }

  public void terminateInstances(
      ImplyEnvironmentConfig implyConfig,
      TerminateInstancesRequest requestBody
  ) throws IOException
  {
    URL requestUrl = new URL(
        StringUtils.format(
            "https://%s/v1/druid/%s/%s",
            implyConfig.getImplyAddress(),
            implyConfig.getClusterId(),
            TERMINATE_PATH
        )
    );

    final Request request = Request(HttpMethod.POST, requestUrl);
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(requestBody));
    doRequest(request);
  }

  public ListInstancesResponse listInstances(
      ImplyEnvironmentConfig implyConfig,
      List<Pair> queryParams
  ) throws IOException
  {
    final String basePath = StringUtils.format(
        "https://%s/v1/druid/%s/%s",
        implyConfig.getImplyAddress(),
        implyConfig.getClusterId(),
        LIST_PATH
    )
    final StringBuilder url = new StringBuilder();
    url.append(basePath);

    if (queryParams != null && !queryParams.isEmpty()) {
      // support (constant) query string in `path`, e.g. "/posts?draft=1"
      String prefix = "?";
      for (Pair param : queryParams) {
        if (param.getValue() != null) {
          if (prefix != null) {
            url.append(prefix);
            prefix = null;
          } else {
            url.append("&");
          }
          url.append(StringUtils.urlEncode(param.getName())).append("=").append(StringUtils.urlEncode(param.getValue()));
        }
      }
    }

    URL requestUrl = new URL(url.toString());

    final Request request = Request(HttpMethod.GET, requestUrl);
    return doRequest(request, ListInstancesResponse.class);
  }

  private <T> T doRequest(Request request, Class<T> clazz) throws IOException
  {
    InputStreamFullResponseHolder responseHolder = doRequest(request);
    return jsonMapper.readValue(responseHolder.getContent(), clazz);
  }

  private InputStreamFullResponseHolder doRequest(Request request)
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
      throw new RE(
          "Failed to talk to node at [%s]. Error code[%d], description[%s].",
          request.getUrl(),
          responseHolder.getStatus().getCode(),
          responseHolder.getStatus().getReasonPhrase()
      );
    }
    return responseHolder;
  }
}
