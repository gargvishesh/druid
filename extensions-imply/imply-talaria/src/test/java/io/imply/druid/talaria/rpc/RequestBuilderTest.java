/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.segment.TestHelper;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.net.URI;

public class RequestBuilderTest
{
  @Test
  public void test_constructor_noLeadingSlash()
  {
    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new RequestBuilder(HttpMethod.GET, "q")
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Path must start with '/'"))
    );
  }

  @Test
  public void test_build_getPlaintext() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.GET, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 8888, -1, ""));

    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(new URI("http://example.com:8888/q").toURL(), request.getUrl());
    Assert.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assert.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assert.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_getTls() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.GET, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assert.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assert.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assert.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_getTlsWithBasePath() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.GET, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 9999, 8888, "/base")) /* TLS preferred over plaintext */;

    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(new URI("https://example.com:8888/base/q").toURL(), request.getUrl());
    Assert.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assert.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assert.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_postTlsNoContent() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.POST, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assert.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assert.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assert.assertFalse(request.hasContent());
  }

  @Test
  public void test_build_postTlsWithContent() throws Exception
  {
    final String json = "{\"foo\": 3}";
    final Request request = new RequestBuilder(HttpMethod.POST, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .content("application/json", StringUtils.toUtf8(json))
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assert.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assert.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assert.assertTrue(request.hasContent());

    // Read and verify content.
    Assert.assertEquals(
        json,
        StringUtils.fromUtf8(ByteStreams.toByteArray(new ChannelBufferInputStream(request.getContent())))
    );
  }

  @Test
  public void test_build_postTlsWithJsonContent() throws Exception
  {
    final Request request = new RequestBuilder(HttpMethod.POST, "/q")
        .header("x-test-header", "abc")
        .header("x-test-header-2", "def")
        .content("application/json", TestHelper.makeJsonMapper(), ImmutableMap.of("foo", 3))
        .build(new ServiceLocation("example.com", 9999, 8888, "")) /* TLS preferred over plaintext */;

    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(new URI("https://example.com:8888/q").toURL(), request.getUrl());
    Assert.assertEquals("abc", Iterables.getOnlyElement(request.getHeaders().get("x-test-header")));
    Assert.assertEquals("def", Iterables.getOnlyElement(request.getHeaders().get("x-test-header-2")));
    Assert.assertTrue(request.hasContent());

    // Read and verify content.
    Assert.assertEquals(
        "{\"foo\":3}",
        StringUtils.fromUtf8(ByteStreams.toByteArray(new ChannelBufferInputStream(request.getContent())))
    );
  }

  @Test
  public void test_timeout()
  {
    Assert.assertEquals(RequestBuilder.DEFAULT_TIMEOUT, new RequestBuilder(HttpMethod.GET, "/q").getTimeout());
    Assert.assertEquals(
        Duration.standardSeconds(1),
        new RequestBuilder(HttpMethod.GET, "/q").timeout(Duration.standardSeconds(1)).getTimeout()
    );
    Assert.assertEquals(
        Duration.ZERO,
        new RequestBuilder(HttpMethod.GET, "/q").timeout(Duration.ZERO).getTimeout()
    );
  }
}
