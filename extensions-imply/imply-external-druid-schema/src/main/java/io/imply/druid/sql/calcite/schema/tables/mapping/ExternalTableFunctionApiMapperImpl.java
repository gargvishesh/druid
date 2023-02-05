/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.external.PolarisExternalTableSpec;
import io.imply.druid.sql.calcite.external.PolarisTableFunctionSpec;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.RetryableException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

/**
 * Table function API mapper is responsible for mapping the supplied Polaris table function spec to the
 * Polaris external table spec by invoking an API call to the table service.
 */
public class ExternalTableFunctionApiMapperImpl implements ExternalTableFunctionMapper
{
  private static final EmittingLogger LOG = new EmittingLogger(ExternalTableFunctionApiMapperImpl.class);

  // A unique prefix tag that Polaris could use to decode the original exception.
  @VisibleForTesting
  public static final String POLARIS_EXCEPTION_TAG = "POLARIS_RETURNED_EXCEPTION";
  private static final Duration POLARIS_POST_TIMEOUT = Duration.millis(5000L);
  private static final int POLARIS_RETRY_COUNT = 5;

  private final ImplyExternalDruidSchemaCommonConfig commonConfig;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  @Inject
  public ExternalTableFunctionApiMapperImpl(
      ImplyExternalDruidSchemaCommonConfig commonSchemaConfig,
      @Json ObjectMapper objectMapper,
      @EscalatedClient HttpClient httpClient
  )
  {
    this.commonConfig = commonSchemaConfig;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
  }

  @Nullable
  @Override
  public PolarisExternalTableSpec getTableFunctionMapping(PolarisTableFunctionSpec serializedTableFnSpec)
  {
    try {
      return postTableMappingCallToPolaris(serializedTableFnSpec);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @VisibleForTesting
  public int getPolarisRetryCount()
  {
    return POLARIS_RETRY_COUNT;
  }

  private PolarisExternalTableSpec postTableMappingCallToPolaris(PolarisTableFunctionSpec polarisTableFunctionSpec)
      throws Exception
  {
    Request req = createRequest(
        new URL(commonConfig.getTableFunctionMappingUrl()),
        objectMapper.writeValueAsBytes(polarisTableFunctionSpec)
    );

    final InputStreamFullResponseHolder responseHolder;
    try {
      responseHolder = RetryUtils.retry(
          () -> doRequest(req),
          e -> e instanceof RetryableException,
          getPolarisRetryCount()
      );
    }
    catch (Exception e) {
      LOG.error(e, "Encountered exception while fetching table mappings");
      throw e;
    }

    return objectMapper.readValue(
        responseHolder.getContent(),
        PolarisExternalTableSpec.class
    );
  }

  private Request createRequest(
      URL listenerURL,
      byte[] serializedEntity
  )
  {
    Request req = new Request(HttpMethod.POST, listenerURL);
    req.setContent(MediaType.APPLICATION_JSON, serializedEntity);
    return req;
  }

  private InputStreamFullResponseHolder doRequest(Request request) throws IOException, RetryableException
  {
    InputStreamFullResponseHolder responseHolder;
    try {
      responseHolder = this.httpClient.go(
          request,
          new InputStreamFullResponseHandler(),
          POLARIS_POST_TIMEOUT
      ).get();
    }
    catch (InterruptedException | ExecutionException e) {
      LOG.error(e, "Exception during %s execution", request.getUrl());
      throw new RuntimeException(e);
    }

    int responseStatusCode = responseHolder.getStatus().getCode();
    if (responseStatusCode == HttpServletResponse.SC_OK) {

      // We are good, just return the response.
      return responseHolder;

    } else if (isRequestRetryable(responseStatusCode)) {

      // Retryable error, wrap and throw a retryable exception with context.
      String exceptionMsg = StringUtils.format(
          "Request to Polaris failed but is retryable %s", exceptionContentAsString(responseHolder.getContent()));
      RetryableException retryableException = new RetryableException(new RuntimeException(exceptionMsg));
      LOG.error(retryableException, "Request failed but is retryable");
      throw retryableException;

    } else {

      // Error, but not going to retry; bubble up the exception as-is with a unique tag.
      String exceptionWithMsg = StringUtils.format("%s %s", POLARIS_EXCEPTION_TAG,
                                                   exceptionContentAsString(responseHolder.getContent())
      );
      IAE polarisException = new IAE(exceptionWithMsg);
      LOG.warn(polarisException, "Polaris returned exception");
      throw polarisException;
    }
  }

  private boolean isRequestRetryable(int responseStatusCode)
  {
    return responseStatusCode == HttpServletResponse.SC_INTERNAL_SERVER_ERROR
           || responseStatusCode == HttpServletResponse.SC_BAD_GATEWAY
           || responseStatusCode == HttpServletResponse.SC_SERVICE_UNAVAILABLE
           || responseStatusCode == HttpServletResponse.SC_GATEWAY_TIMEOUT;
  }

  private String exceptionContentAsString(InputStream content) throws IOException
  {
    return IOUtils.toString(content, StandardCharsets.UTF_8);
  }
}
