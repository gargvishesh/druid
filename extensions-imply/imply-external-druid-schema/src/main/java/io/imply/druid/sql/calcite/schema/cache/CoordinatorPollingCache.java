/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link PollingManagedCache} with a {@link DruidLeaderClient} which can poll a Druid coordinator to fetch some set
 * of information location at a Coordinator HTTP path.
 */
public abstract class CoordinatorPollingCache<T> extends PollingManagedCache<T>
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingCache.class);

  protected final DruidLeaderClient druidLeaderClient;
  protected final String coordinatorPath;

  public CoordinatorPollingCache(
      ImplyExternalDruidSchemaCommonConfig commonConfig,
      ObjectMapper objectMapper,
      DruidLeaderClient coordinatorLeaderClient,
      String cacheName,
      String coordinatorPath
  )
  {
    super(cacheName, commonConfig, objectMapper);
    this.druidLeaderClient = coordinatorLeaderClient;
    this.coordinatorPath = coordinatorPath;
  }

  @Override
  protected byte[] tryFetchDataForPath(String path) throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        path
    );
    BytesFullResponseHolder responseHolder = druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    if (!HttpResponseStatus.OK.equals(responseHolder.getStatus())) {
      LOG.warn(
          "Got an error status when fetching schemas from coordinator: %s, content: %s",
          responseHolder.getStatus(),
          responseHolder.getContent()
      );
      return null;
    }
    return responseHolder.getContent();
  }
}
