/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.mapping;

import com.google.common.annotations.VisibleForTesting;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.net.URL;

/**
 * Table function API mapper is responsible for mapping the supplied table functiom spec to the
 * serialized Polaris external table spec by invoking API calls to the table service.
 */
public class ExternalTableFunctionApiMapperImpl implements ExternalTableFunctionMapper
{
  private static final EmittingLogger LOG = new EmittingLogger(ExternalTableFunctionApiMapperImpl.class);
  private final HttpClient httpClient;
  private final ImplyExternalDruidSchemaCommonConfig commonConfig;

  @Inject
  public ExternalTableFunctionApiMapperImpl(
      ImplyExternalDruidSchemaCommonConfig commonSchemaConfig,
      @EscalatedClient HttpClient httpClient
  )
  {
    this.commonConfig = commonSchemaConfig;
    this.httpClient = httpClient;
  }

  @Nullable
  @Override
  public byte[] getTableFunctionMapping(byte[] serializedTableFnSpec)
  {
    try {
      return RetryUtils.retry(
          () -> postTableMappingCallToPolaris(serializedTableFnSpec),
          e -> true,
          this.commonConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.error("Exception occurred during getTableFunctionMapping " + e);
      throw new IAE(StringUtils.format("Exception occurred while fetching table function mapping %s", e));
    }
  }

  @VisibleForTesting
  public byte[] postTableMappingCallToPolaris(byte[] serializedTableFnSpec)
      throws Exception
  {
    Request req = createRequest(new URL(commonConfig.getTableFunctionMappingUrl()), serializedTableFnSpec);
    final BytesFullResponseHolder is = httpClient.go(
        req,
        new BytesFullResponseHandler()
    ).get();
    return is.getContent();
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
}
