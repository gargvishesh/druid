/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;

/**
 * Production implementation of {@link CoordinatorServiceClient}.
 */
public class CoordinatorServiceClientImpl implements CoordinatorServiceClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public CoordinatorServiceClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = Preconditions.checkNotNull(client, "client");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
  }

  @Override
  public ListenableFuture<DataSegment> fetchUsedSegment(String dataSource, String segmentId)
  {
    final String path = StringUtils.format(
        "/druid/coordinator/v1/metadata/datasources/%s/segments/%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(segmentId)
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<DataSegment>() {})
    );
  }

  @Override
  public CoordinatorServiceClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new CoordinatorServiceClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }

  /**
   * Deserialize a {@link BytesFullResponseHolder} as JSON.
   *
   * It would be reasonable to move this to {@link BytesFullResponseHolder} itself, or some shared utility class.
   */
  private <T> T deserialize(final BytesFullResponseHolder bytesHolder, final TypeReference<T> typeReference)
  {
    try {
      return jsonMapper.readValue(bytesHolder.getContent(), typeReference);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
