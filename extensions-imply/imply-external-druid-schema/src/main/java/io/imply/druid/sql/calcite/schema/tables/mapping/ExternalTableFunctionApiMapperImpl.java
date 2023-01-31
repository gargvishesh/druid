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
import io.imply.druid.sql.calcite.external.PolarisExternalTableSpec;
import io.imply.druid.sql.calcite.external.PolarisTableFunctionSpec;
import io.imply.druid.sql.calcite.external.exception.WrappedErrorModelException;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;

/**
 * Table function API mapper is responsible for mapping the supplied table functiom spec to the
 * serialized Polaris external table spec by invoking API calls to the table service.
 */
public class ExternalTableFunctionApiMapperImpl implements ExternalTableFunctionMapper
{
  private static final EmittingLogger LOG = new EmittingLogger(ExternalTableFunctionApiMapperImpl.class);
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final ImplyExternalDruidSchemaCommonConfig commonConfig;

  @Inject
  public ExternalTableFunctionApiMapperImpl(
      ImplyExternalDruidSchemaCommonConfig commonSchemaConfig,
      ObjectMapper objectMapper,
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

  @VisibleForTesting
  public PolarisExternalTableSpec postTableMappingCallToPolaris(PolarisTableFunctionSpec polarisTableFunctionSpec)
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

  @VisibleForTesting
  public Request createRequest(
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
          new InputStreamFullResponseHandler()
      ).get();
    }
    catch (InterruptedException | ExecutionException e) {
      LOG.warn("Exception during execution: %s", e);
      throw new RuntimeException(e);
    }

    if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK
        && responseHolder.getStatus().getCode() != HttpServletResponse.SC_INTERNAL_SERVER_ERROR) {
      // TODO: very likely this type, but if this deserialization fails, fallback to a generic runtime exception perhaps
      WrappedErrorModelException wrappedException = objectMapper.readValue(
          responseHolder.getContent(),
          WrappedErrorModelException.class
      );
      LOG.warn("Exception from Polaris %s", wrappedException.getImplyError());
      throw wrappedException;
    }
    return responseHolder;
  }
}
