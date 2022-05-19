/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.http.client.Request;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * Used by {@link DruidServiceClient} to generate {@link Request} objects for an
 * {@link org.apache.druid.java.util.http.client.HttpClient}.
 */
public class RequestBuilder
{
  @VisibleForTesting
  static final Duration DEFAULT_TIMEOUT = Duration.standardMinutes(2);

  private final HttpMethod method;
  private final String encodedPathAndQueryString;
  private final Multimap<String, String> headers = HashMultimap.create();
  private String contentType = null;
  private byte[] content = null;
  private Duration timeout = DEFAULT_TIMEOUT;

  public RequestBuilder(final HttpMethod method, final String encodedPathAndQueryString)
  {
    this.method = Preconditions.checkNotNull(method, "method");
    this.encodedPathAndQueryString = Preconditions.checkNotNull(encodedPathAndQueryString, "encodedPathAndQueryString");

    if (!encodedPathAndQueryString.startsWith("/")) {
      throw new IAE("Path must start with '/'");
    }
  }

  public RequestBuilder header(final String header, final String value)
  {
    headers.put(header, value);
    return this;
  }

  public RequestBuilder content(final String contentType, final byte[] content)
  {
    this.contentType = Preconditions.checkNotNull(contentType, "contentType");
    this.content = Preconditions.checkNotNull(content, "content");
    return this;
  }

  public RequestBuilder content(final String contentType, final ObjectMapper jsonMapper, final Object content)
  {
    try {
      this.contentType = Preconditions.checkNotNull(contentType, "contentType");
      this.content = jsonMapper.writeValueAsBytes(Preconditions.checkNotNull(content, "content"));
      return this;
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public RequestBuilder timeout(final Duration timeout)
  {
    this.timeout = Preconditions.checkNotNull(timeout, "timeout");
    return this;
  }

  /**
   * Accessor for request timeout. Provided because the timeout is not part of the request generated
   * by {@link #build(ServiceLocation)}.
   *
   * If there is no timeout, returns an empty Duration.
   */
  public Duration getTimeout()
  {
    return timeout;
  }

  public Request build(ServiceLocation serviceLocation)
  {
    // It's expected that our encodedPathAndQueryString starts with '/' and the service base path doesn't end with one.
    final String path = serviceLocation.getBasePath() + encodedPathAndQueryString;
    final Request request = new Request(method, makeURL(serviceLocation, path));

    for (final Map.Entry<String, String> entry : headers.entries()) {
      request.addHeader(entry.getKey(), entry.getValue());
    }

    if (contentType != null) {
      request.setContent(contentType, content);
    }

    return request;
  }

  private URL makeURL(final ServiceLocation serviceLocation, final String encodedPathAndQueryString)
  {
    final String scheme;
    final int portToUse;

    if (serviceLocation.getTlsPort() > 0) {
      // Prefer HTTPS if available.
      scheme = "https";
      portToUse = serviceLocation.getTlsPort();
    } else {
      scheme = "http";
      portToUse = serviceLocation.getPlaintextPort();
    }

    // Use URL constructor, not URI, since the path is already encoded.
    try {
      return new URL(scheme, serviceLocation.getHost(), portToUse, encodedPathAndQueryString);
    }
    catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
