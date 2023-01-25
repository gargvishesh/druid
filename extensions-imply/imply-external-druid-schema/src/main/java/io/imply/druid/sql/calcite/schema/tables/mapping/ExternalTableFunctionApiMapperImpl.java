/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */
package io.imply.druid.sql.calcite.schema.tables.mapping;

import java.net.MalformedURLException;
import java.net.URL;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;

import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;

/**
 * Table function API mapper is responsible for mapping the supplied table functiom spec to the
 * serialized Polaris external table spec by invoking API calls to the table service.
 */
public class ExternalTableFunctionApiMapperImpl implements ExternalTableFunctionMapper {
  private static final EmittingLogger LOG = new EmittingLogger(ExternalTableFunctionApiMapperImpl.class);
  private final HttpClient httpClient;
  private final ImplyExternalDruidSchemaCommonCacheConfig schemaConfig;

  public ExternalTableFunctionApiMapperImpl(
      ImplyExternalDruidSchemaCommonCacheConfig commonSchemaConfig,
      @EscalatedClient HttpClient httpClient
  ) {
    this.schemaConfig = commonSchemaConfig;
    this.httpClient = httpClient;
  }

  @Override
  public byte[] getTableFunctionMapping(byte[] serializedTableFnSpec) {
    try {
      return RetryUtils.retry(
          () -> tryFetchDataForPath(serializedTableFnSpec),
          e -> true,
          this.schemaConfig.getMaxSyncRetries()
      );
    } catch (Exception e) {
      LOG.error("Exception occurred during getTableFunctionMapping " + e);
    }
    return null;
  }

  private byte[] tryFetchDataForPath(byte[] serializedTableFnSpec)
      throws Exception {
    final BytesFullResponseHolder is = httpClient.go(
        new Request(HttpMethod.POST, getTableFunctionMappingUrl())
            .setContent(SmileMediaTypes.APPLICATION_JACKSON_SMILE, serializedTableFnSpec),
        new BytesFullResponseHandler()
    ).get();
    return is.getContent();
  }


  private URL getTableFunctionMappingUrl() throws MalformedURLException {
    return new URL(StringUtils.format(
        "%s/v2/jobsDML/internal/tableFunctionMapping",
        this.schemaConfig.getTablesServiceUrl()
    ));
  }
}
