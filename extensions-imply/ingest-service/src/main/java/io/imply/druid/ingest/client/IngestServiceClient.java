/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.imply.druid.ingest.IngestService;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.server.IngestJobRequest;
import io.imply.druid.ingest.server.IngestJobsResponse;
import io.imply.druid.ingest.server.SchemasResponse;
import io.imply.druid.ingest.server.TablesResponse;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class IngestServiceClient
{
  private static final NodeRole INGEST_NODE_ROLE = new NodeRole(IngestService.SERVICE_NAME);

  public static final String SELF_DISCO = "status/selfDiscovered";
  public static final String INGEST_TABLES_PATH = "ingest/v1/tables";
  public static final String INGEST_TABLES_TEMPLATE = "ingest/v1/tables/%s";
  public static final String INGEST_TABLES_TEMPLATE_JOB_TEMPLATE = "ingest/v1/tables/%s/jobs";
  public static final String INGEST_TABLES_TEMPLATE_SAMPLE_TEMPLATE = "ingest/v1/tables/%s/jobs/%s/sample";
  public static final String INGEST_TABLES_TEMPLATE_SUBMIT_TEMPLATE = "ingest/v1/tables/%s/jobs/%s";
  public static final String INGEST_JOBS_PATH = "ingest/v1/jobs";
  public static final String INGEST_SCHEMAS_PATH = "ingest/v1/schemas";

  private final HttpClient client;
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final ObjectMapper jsonMapper;

  public IngestServiceClient(HttpClient client, DruidNodeDiscoveryProvider discoveryProvider, ObjectMapper jsonMapper)
  {
    this.client = client;
    this.discoveryProvider = discoveryProvider;
    this.jsonMapper = jsonMapper;
  }

  public boolean isSelfDiscovered() throws IOException, ExecutionException, InterruptedException
  {
    DiscoveryDruidNode node = pickNode();
    final Request request = getRequestForNode(node, HttpMethod.GET, SELF_DISCO);

    StatusResponseHolder response = client.go(
        request,
        StatusResponseHandler.getInstance()
    ).get();

    return response.getStatus().equals(HttpResponseStatus.OK);
  }

  public TablesResponse getTables() throws IOException
  {
    return getRequest(INGEST_TABLES_PATH, TablesResponse.class);
  }

  public boolean createTable(String table) throws MalformedURLException, ExecutionException, InterruptedException
  {
    DiscoveryDruidNode node = pickNode();
    final Request request = getRequestForNode(node, HttpMethod.POST, StringUtils.format(INGEST_TABLES_TEMPLATE, table));

    StatusResponseHolder response = client.go(
        request,
        StatusResponseHandler.getInstance()
    ).get();

    return response.getStatus().getCode() == HttpServletResponse.SC_CREATED;
  }

  public String stageJob(String table, JobRunner jobRunner)
      throws MalformedURLException, JsonProcessingException, ExecutionException, InterruptedException
  {
    DiscoveryDruidNode node = pickNode();
    final Request request = getRequestForNode(node, HttpMethod.POST, StringUtils.format(INGEST_TABLES_TEMPLATE_JOB_TEMPLATE, table));
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(jobRunner));

    StatusResponseHolder response = client.go(
        request,
        StatusResponseHandler.getInstance()
    ).get();

    return response.getContent();
  }

  public SamplerResponse sampleJob(String table, String jobId, IngestJobRequest jobRequest)
      throws IOException
  {
    DiscoveryDruidNode node = pickNode();
    final Request request = getRequestForNode(node, HttpMethod.POST, StringUtils.format(INGEST_TABLES_TEMPLATE_SAMPLE_TEMPLATE, table, jobId));
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(jobRequest));

    return doRequest(request, SamplerResponse.class);
  }

  public boolean submitJob(String table, String jobId, IngestJobRequest jobRequest)
      throws IOException
  {
    DiscoveryDruidNode node = pickNode();
    final Request request = getRequestForNode(node, HttpMethod.POST, StringUtils.format(INGEST_TABLES_TEMPLATE_SUBMIT_TEMPLATE, table, jobId));
    request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(jobRequest));

    InputStreamFullResponseHolder holder = doRequest(request);
    return holder.getStatus().getCode() == HttpServletResponse.SC_CREATED;
  }

  public SchemasResponse getSchemas() throws IOException
  {
    return getRequest(INGEST_SCHEMAS_PATH, SchemasResponse.class);
  }

  public IngestJobsResponse getJobs() throws IOException
  {
    return getRequest(INGEST_JOBS_PATH, IngestJobsResponse.class);
  }

  private <T> T getRequest(String path, Class<T> clazz) throws IOException
  {
    DiscoveryDruidNode node = pickNode();
    Request request = getRequestForNode(node, HttpMethod.GET, path);
    return doRequest(request, clazz);
  }

  private <T> T doRequest(Request request, Class<T> clazz) throws IOException
  {
    InputStreamFullResponseHolder responseHolder = doRequest(request);
    if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK) {
      throw new RE(
          "Failed to talk to node at [%s]. Error code[%d], description[%s].",
          request.getUrl(),
          responseHolder.getStatus().getCode(),
          responseHolder.getStatus().getReasonPhrase()
      );
    }
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

    return responseHolder;
  }



  protected DiscoveryDruidNode pickNode()
  {
    Collection<DiscoveryDruidNode> ingestNodes = discoveryProvider.getForNodeRole(INGEST_NODE_ROLE)
                                                                  .getAllNodes();
    Iterator<DiscoveryDruidNode> nodeIterator = ingestNodes.iterator();
    if (nodeIterator.hasNext()) {
      final int skip = ingestNodes.size() > 1 ? ThreadLocalRandom.current().nextInt(ingestNodes.size()) : 0;
      DiscoveryDruidNode node;
      int count = 0;
      do {
        node = nodeIterator.next();
      } while (count++ < skip && nodeIterator.hasNext());
      return node;
    }
    throw new RE(
        "No [%s] servers available to query, make sure the service is running and announced",
        IngestService.SERVICE_NAME
    );
  }

  @VisibleForTesting
  static Request getRequestForNode(DiscoveryDruidNode node, HttpMethod method, String path) throws MalformedURLException
  {
    URL jobsQuery = new URL(
        StringUtils.format(
            "%s://%s/%s",
            node.getDruidNode().getServiceScheme(),
            node.getDruidNode().getHostAndPortToUse(),
            path
        )
    );

    return new Request(method, jobsQuery);
  }
}
