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
import io.imply.druid.sql.calcite.external.PolarisExternalTableSpec;
import io.imply.druid.sql.calcite.external.PolarisTableFunctionSpec;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
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
  private final ImplyExternalDruidSchemaCommonConfig commonConfig;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  // A unique prefix tag that Polaris could use to decode the original exception.
  private static final String POLARIS_EXCEPTION_TAG = "POLARIS_RETURNED_EXCEPTION";
  private static final Duration POLARIS_POST_TIMEOUT = Duration.millis(5000L);

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
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private PolarisExternalTableSpec postTableMappingCallToPolaris(PolarisTableFunctionSpec polarisTableFunctionSpec)
      throws IOException
  {
    Request req = createRequest(
        new URL(commonConfig.getTableFunctionMappingUrl()),
        objectMapper.writeValueAsBytes(polarisTableFunctionSpec)
    );
    InputStreamFullResponseHolder responseHolder = doRequest(req);
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

  private InputStreamFullResponseHolder doRequest(Request request) throws IOException
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
      LOG.warn(e, "Exception during %s execution", request.getUrl());
      throw new RuntimeException(e);
    }
    if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK) {
      IAE polarisException = new IAE("%s %s", POLARIS_EXCEPTION_TAG,
                                     IOUtils.toString(responseHolder.getContent(), StandardCharsets.UTF_8));
      LOG.warn(polarisException, "Polaris returned exception");
      throw polarisException;
    }
    return responseHolder;
  }
}
