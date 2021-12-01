/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.imply.druid.sql.async.AsyncQueryPoolConfig;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AsyncResourceTestClient
{
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String broker;

  @Inject
  AsyncResourceTestClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.broker = config.getBrokerUrl();
  }

  private String getBrokerURL()
  {
    return StringUtils.format(
        "%s/druid/v2/",
        broker
    );
  }

  public SqlAsyncQueryDetailsApiResponse submitAsyncQuery(final SqlQuery query) throws Exception
  {
    String url = StringUtils.format("%ssql/async/", getBrokerURL());
    Request request = new Request(HttpMethod.POST, new URL(url));
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(query));

    BytesFullResponseHolder response = httpClient.go(
        request,
        new BytesFullResponseHandler()
    ).get();

    if (response.getStatus().equals(HttpResponseStatus.TOO_MANY_REQUESTS)) {
      throw QueryCapacityExceededException.withErrorMessageAndResolvedHost("");
    } else if (!response.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
      throw new ISE(
          "Error while submiting async query. status[%s] content[%s]",
          response.getStatus(),
          new String(response.getContent(), StandardCharsets.UTF_8)
      );
    }

    return jsonMapper.readValue(response.getContent(), new TypeReference<SqlAsyncQueryDetailsApiResponse>() {});
  }

  public SqlAsyncQueryDetailsApiResponse getStatus(final String asyncResultId) throws Exception
  {
    String url = StringUtils.format("%ssql/async/%s/status", getBrokerURL(), asyncResultId);
    Request request = new Request(HttpMethod.GET, new URL(url));

    BytesFullResponseHolder response = httpClient.go(
        request,
        new BytesFullResponseHandler()
    ).get();

    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
      return null;
    }

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting status for async query status[%s] content[%s]",
          response.getStatus(),
          new String(response.getContent(), StandardCharsets.UTF_8)
      );
    }

    return jsonMapper.readValue(response.getContent(), new TypeReference<SqlAsyncQueryDetailsApiResponse>() {});
  }

  public <T> List<T> getResults(final String asyncResultId) throws Exception
  {
    String url = StringUtils.format("%ssql/async/%s/results", getBrokerURL(), asyncResultId);
    Request request = new Request(HttpMethod.GET, new URL(url));

    BytesFullResponseHolder response = httpClient.go(
        request,
        new BytesFullResponseHandler()
    ).get();

    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
      return null;
    }

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting status for async query status[%s] content[%s]",
          response.getStatus(),
          new String(response.getContent(), StandardCharsets.UTF_8)
      );
    }

    return jsonMapper.readValue(response.getContent(), new TypeReference<List<T>>() {});
  }

  public boolean cancel(final String asyncResultId)
      throws MalformedURLException, ExecutionException, InterruptedException
  {
    String url = StringUtils.format("%ssql/async/%s", getBrokerURL(), asyncResultId);

    Request request = new Request(HttpMethod.DELETE, new URL(url));

    StatusResponseHolder response = httpClient.go(
        request,
        StatusResponseHandler.getInstance()
    ).get();

    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
      return false;
    }

    if (!response.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
      throw new ISE(
          "Got [%s] while cancelling async query [%s]",
          response.getStatus(),
          asyncResultId
      );
    }

    return true;
  }

  public AsyncQueryPoolConfig getAsyncQueryPoolConfig()
  {
    // The limit configs are set in integration-tests-imply/docker/environment-configs/common-async-download
    // The hardcoded values below must be keep in sync with the configs in the configuration file mentioned above.
    return new AsyncQueryPoolConfig(10, 3);
  }
}
