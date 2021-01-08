/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.clarity.emitter.ClarityNodeDetails;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.utils.CompressionUtils;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public class ClarityHttpEmitterTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final String TARGET_URL = "http://metrics.foo.bar/";
  private static final String TEST_NODE_TYPE = "test-node-type";
  private static final String TEST_IMPLY_VERSION = "test-imply-version"; // must match value in /resources/imply.version
  private static final String TEST_IMPLY_CLUSTER = "test-imply-cluster";
  private static final String TARGET_URL_KEY = "test";

  private static String TEST_DRUID_VERSION;

  private MockHttpClient httpClient;
  private ClarityHttpEmitter emitter;

  public static StatusResponseHolder okResponse()
  {
    return new StatusResponseHolder(
        new HttpResponseStatus(201, "Created"),
        new StringBuilder("Yay")
    );
  }

  @BeforeClass
  public static void setUpClass()
  {
    String implementationVersion = DruidMetrics.class.getPackage().getImplementationVersion();
    TEST_DRUID_VERSION = implementationVersion == null ? "unknown" : implementationVersion;
  }

  @Before
  public void setUp()
  {
    httpClient = new MockHttpClient();
  }

  @After
  public void tearDown() throws Exception
  {
    if (emitter != null) {
      emitter.close();
    }
  }

  @Test
  public void testSanity() throws Exception
  {
    final List<UnitEvent> events = Arrays.asList(buildUnitEvent(1), buildUnitEvent(2));
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withFlushCount(2)
                                .build()
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          ) throws Exception
          {
            Assert.assertTrue("handler is a StatusResponseHandler", handler instanceof StatusResponseHandler);
            Assert.assertEquals(new URL(TARGET_URL), request.getUrl());
            Assert.assertEquals(
                ImmutableList.of("application/json"),
                request.getHeaders().get(HttpHeaders.Names.CONTENT_TYPE)
            );

            Assert.assertEquals(
                JSON_MAPPER.convertValue(ImmutableList.of(events.get(0).toMap(), events.get(1).toMap()), List.class),
                JSON_MAPPER.readValue(request.getContent().toString(StandardCharsets.UTF_8), List.class)
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    waitForEmission(emitter);
    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testSizeBasedEmission() throws Exception
  {
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withFlushCount(3)
                                .build()
    );

    httpClient.setGoHandler(assertingFailingHandler());
    emitter.emit(buildUnitEvent(1));
    emitter.emit(buildUnitEvent(2));

    httpClient.setGoHandler(GoHandlers.passingHandler(okResponse()).times(1));
    emitter.emit(buildUnitEvent(3));
    waitForEmission(emitter);

    httpClient.setGoHandler(assertingFailingHandler());
    emitter.emit(buildUnitEvent(4));
    emitter.emit(buildUnitEvent(5));

    closeAndExpectFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testTimeBasedEmission() throws Exception
  {
    final int timeBetweenEmissions = 100;
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withFlushMillis(timeBetweenEmissions)
                                .build()
    );

    final CountDownLatch latch = new CountDownLatch(1);

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request intermediateFinalRequest,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          )
          {
            latch.countDown();
            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    long emitTime = System.currentTimeMillis();
    emitter.emit(buildUnitEvent(1));

    latch.await();
    long timeWaited = System.currentTimeMillis() - emitTime;
    Assert.assertTrue(
        String.format(Locale.ENGLISH, "timeWaited[%s] !< %s", timeWaited, timeBetweenEmissions * 2),
        timeWaited < timeBetweenEmissions * 2
    );

    waitForEmission(emitter);

    final CountDownLatch thisLatch = new CountDownLatch(1);
    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request intermediateFinalRequest,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          )
          {
            thisLatch.countDown();
            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    emitTime = System.currentTimeMillis();
    emitter.emit(buildUnitEvent(2));

    thisLatch.await();
    timeWaited = System.currentTimeMillis() - emitTime;
    Assert.assertTrue(
        String.format(Locale.ENGLISH, "timeWaited[%s] !< %s", timeWaited, timeBetweenEmissions * 2),
        timeWaited < timeBetweenEmissions * 2
    );

    waitForEmission(emitter);
    closeNoFlush(emitter);
    Assert.assertTrue("httpClient.succeeded()", httpClient.succeeded());
  }

  @Test
  public void testFailedEmission() throws Exception
  {
    final UnitEvent event1 = buildUnitEvent(1);
    final UnitEvent event2 = buildUnitEvent(2);
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withFlushCount(1)
                                .build()
    );

    Assert.assertEquals(0, emitter.getBufferedSize());

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          )
          {
            final Intermediate obj = handler
                .handleResponse(
                    new DefaultHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.BAD_REQUEST
                    ),
                    null
                )
                .getObj();
            Assert.assertNotNull(obj);
            return Futures.immediateFuture((Final) obj);
          }
        }.times(1)
    );
    emitter.emit(event1);
    waitForEmission(emitter);
    Assert.assertTrue(httpClient.succeeded());

    // The event should still be queued up, since it failed and will want to be retried.
    Assert.assertEquals(JSON_MAPPER.writeValueAsString(event1).length(), emitter.getBufferedSize());

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          )
              throws Exception
          {
            Assert.assertEquals(
                JSON_MAPPER.convertValue(
                    ImmutableList.of(event1.toMap(), event2.toMap()), List.class
                ),
                JSON_MAPPER.readValue(request.getContent().toString(StandardCharsets.UTF_8), List.class)
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    emitter.emit(event2);
    waitForEmission(emitter);

    // Nothing should be queued up, since everything has succeeded.
    Assert.assertEquals(0, emitter.getBufferedSize());

    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testBasicAuthenticationAndNewlineSeparating() throws Exception
  {
    final List<UnitEvent> events = Arrays.asList(buildUnitEvent(1), buildUnitEvent(2));

    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withBatchingStrategy(BatchingStrategy.NEWLINES)
                                .withBasicAuthentication("foo:bar")
                                .build()
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          ) throws Exception
          {
            Assert.assertEquals(new URL(TARGET_URL), request.getUrl());
            Assert.assertEquals(
                ImmutableList.of("application/json"),
                request.getHeaders().get(HttpHeaders.Names.CONTENT_TYPE)
            );
            Assert.assertEquals(
                ImmutableList.of("Basic " + StringUtils.utf8Base64("foo:bar")),
                request.getHeaders().get(HttpHeaders.Names.AUTHORIZATION)
            );

            String[] lines = request.getContent().toString(StandardCharsets.UTF_8).split("\n");

            Assert.assertEquals(
                ImmutableList.of(events.get(0).toMap(), events.get(1).toMap()),
                ImmutableList.of(JSON_MAPPER.readValue(lines[0], Map.class), JSON_MAPPER.readValue(lines[1], Map.class))
            );

            Assert.assertTrue(
                "handler is a StatusResponseHandler",
                handler instanceof StatusResponseHandler
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    emitter.flush();
    waitForEmission(emitter);
    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testBatchSplitting() throws Exception
  {
    final byte[] big = new byte[500 * 1024];
    for (int i = 0; i < big.length; i++) {
      big[i] = 'x';
    }
    final String bigString = new String(big, StandardCharsets.UTF_8);
    final List<UnitEvent> events = Arrays.asList(
        buildUnitEvent(bigString, 1),
        buildUnitEvent(bigString, 2),
        buildUnitEvent(bigString, 3),
        buildUnitEvent(bigString, 4)
    );

    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withMaxBatchSize(1024 * 1024)
                                .withMaxBufferSize(5 * 1024 * 1024)
                                .withFlushBufferPercentFull(100)
                                .build()
    );

    Assert.assertEquals(0, emitter.getBufferedSize());

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          ) throws Exception
          {
            Assert.assertTrue("handler is a StatusResponseHandler", handler instanceof StatusResponseHandler);
            Assert.assertEquals(new URL(TARGET_URL), request.getUrl());
            Assert.assertEquals(
                ImmutableList.of("application/json"),
                request.getHeaders().get(HttpHeaders.Names.CONTENT_TYPE)
            );

            Assert.assertEquals(
                2,
                JSON_MAPPER.readValue(request.getContent().toString(StandardCharsets.UTF_8), List.class).size()
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(3)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    Assert.assertEquals(JSON_MAPPER.writeValueAsString(events).length() - events.size() - 1, emitter.getBufferedSize());

    emitter.flush();
    waitForEmission(emitter);
    Assert.assertEquals(0, emitter.getBufferedSize());
    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testFlushWhenBufferFull() throws Exception
  {
    final byte[] big = new byte[500 * 1024];
    for (int i = 0; i < big.length; i++) {
      big[i] = 'x';
    }
    final String bigString = new String(big, StandardCharsets.UTF_8);

    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withMaxBatchSize(5 * 512 * 1024)
                                .withMaxBufferSize(8 * 512 * 1024)
                                .withFlushBufferPercentFull(50)
                                .build()
    );

    Assert.assertEquals(0, emitter.getBufferedSize());

    httpClient.setGoHandler(assertingFailingHandler());

    emitter.emit(buildUnitEvent(bigString, 1));
    emitter.emit(buildUnitEvent(bigString, 2));
    emitter.emit(buildUnitEvent(bigString, 3));
    emitter.emit(buildUnitEvent(bigString, 4));

    httpClient.setGoHandler(GoHandlers.passingHandler(okResponse()).times(1));
    emitter.emit(buildUnitEvent(bigString, 5));
    waitForEmission(emitter);

    httpClient.setGoHandler(assertingFailingHandler());
    emitter.emit(buildUnitEvent(bigString, 6));
    emitter.emit(buildUnitEvent(bigString, 7));

    closeAndExpectFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testLz4Compression() throws Exception
  {
    final List<UnitEvent> events = Arrays.asList(buildUnitEvent(1), buildUnitEvent(2));
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withFlushCount(2)
                                .build()
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          ) throws Exception
          {
            LZ4BlockInputStream is = new LZ4BlockInputStream(new ByteArrayInputStream(request.getContent().array()));

            Assert.assertEquals(
                JSON_MAPPER.convertValue(ImmutableList.of(events.get(0).toMap(), events.get(1).toMap()), List.class),
                JSON_MAPPER.readValue(IOUtils.toString(is, StandardCharsets.UTF_8), List.class)
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    waitForEmission(emitter);
    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testGzipCompression() throws Exception
  {
    final List<UnitEvent> events = Arrays.asList(buildUnitEvent(1), buildUnitEvent(2));
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.GZIP)
                                .withFlushCount(2)
                                .build()
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          ) throws Exception
          {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CompressionUtils.gunzip(new ByteArrayInputStream(request.getContent().array()), baos);

            Assert.assertEquals(
                JSON_MAPPER.convertValue(ImmutableList.of(events.get(0).toMap(), events.get(1).toMap()), List.class),
                JSON_MAPPER.readValue(baos.toString(StandardCharsets.UTF_8.name()), List.class)
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    waitForEmission(emitter);
    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testSampledMetrics0() throws Exception
  {
    List<UnitEvent> events = ImmutableList.of(
        buildUnitEvent("test", 1, "metric-type-1", "id-1"),
        buildUnitEvent("test", 2, "metric-type-2", "id-2"),
        buildUnitEvent("test", 3, "metric-type-1", null),
        buildUnitEvent("test", 4, "metric-type-2", null)
    );

    List expectedResults = JSON_MAPPER.convertValue(
        ImmutableList.of(events.get(1).toMap(), events.get(3).toMap()),
        List.class
    );

    testSampledMetrics(events, expectedResults, Sets.newHashSet("metric-type-1"), Sets.newHashSet(TEST_NODE_TYPE), 0);
  }

  @Test
  public void testSampledMetrics100() throws Exception
  {
    List<UnitEvent> events = ImmutableList.of(
        buildUnitEvent("test", 1, "metric-type-1", "id-1"),
        buildUnitEvent("test", 2, "metric-type-2", "id-2"),
        buildUnitEvent("test", 3, "metric-type-1", null),
        buildUnitEvent("test", 4, "metric-type-2", null)
    );

    List expectedResults = JSON_MAPPER.convertValue(
        ImmutableList.of(events.get(0).toMap(), events.get(1).toMap(), events.get(2).toMap(), events.get(3).toMap()),
        List.class
    );

    testSampledMetrics(events, expectedResults, Sets.newHashSet("metric-type-1"), Sets.newHashSet(TEST_NODE_TYPE), 100);
  }

  @Test
  public void testSampledMetricsNodeType() throws Exception
  {
    List<UnitEvent> events = ImmutableList.of(
        buildUnitEvent("test", 1, "metric-type-1", "id-1"),
        buildUnitEvent("test", 2, "metric-type-2", "id-2"),
        buildUnitEvent("test", 3, "metric-type-1", null),
        buildUnitEvent("test", 4, "metric-type-2", null)
    );

    List expectedResults = JSON_MAPPER.convertValue(
        ImmutableList.of(events.get(0).toMap(), events.get(1).toMap(), events.get(2).toMap(), events.get(3).toMap()),
        List.class
    );

    testSampledMetrics(events, expectedResults, Sets.newHashSet("metric-type-1"), Sets.newHashSet("other-node"), 0);
  }

  private void testSampledMetrics(
      List<UnitEvent> events,
      final List<Object> expectedResults,
      Set<String> sampledMetrics,
      Set<String> sampledNodeTypes,
      int samplingRate
  ) throws Exception
  {
    emitter = getEmitter(
        ClarityHttpEmitterConfig.builder(TARGET_URL)
                                .withClusterName(TEST_IMPLY_CLUSTER)
                                .withCompression(ClarityHttpEmitterConfig.Compression.NONE)
                                .withFlushCount(events.size())
                                .withSampledMetrics(sampledMetrics)
                                .withSampledNodeTypes(sampledNodeTypes)
                                .withSamplingRate(samplingRate)
                                .build()
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public <Intermediate, Final> ListenableFuture<Final> go(
              Request request,
              HttpResponseHandler<Intermediate, Final> handler,
              Duration duration
          ) throws Exception
          {
            Assert.assertEquals(
                expectedResults,
                JSON_MAPPER.readValue(request.getContent().toString(StandardCharsets.UTF_8), List.class)
            );

            return Futures.immediateFuture((Final) okResponse());
          }
        }.times(1)
    );

    for (Event event : events) {
      emitter.emit(event);
    }
    waitForEmission(emitter);
    closeNoFlush(emitter);
    Assert.assertTrue(httpClient.succeeded());
  }

  private static GoHandler assertingFailingHandler()
  {
    return new GoHandler()
    {
      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(
          Request request,
          HttpResponseHandler<Intermediate, Final> handler,
          Duration duration
      )
      {
        Assert.fail("Failing handler was called");
        throw new ISE("Shouldn\'t be called", new Object[0]);
      }
    };
  }

  private void closeAndExpectFlush(Emitter emitter) throws IOException
  {
    httpClient.setGoHandler(GoHandlers.passingHandler(okResponse()).times(1));
    emitter.close();
  }

  private void closeNoFlush(Emitter emitter) throws IOException
  {
    emitter.close();
  }

  private void waitForEmission(ClarityHttpEmitter emitter) throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    emitter.getExec().execute(
        new Runnable()
        {
          @Override
          public void run()
          {
            latch.countDown();
          }
        }
    );

    if (!latch.await(10, TimeUnit.SECONDS)) {
      Assert.fail("latch await() did not complete in 10 seconds");
    }
  }

  private ClarityHttpEmitter getEmitter(ClarityHttpEmitterConfig config)
  {
    ClarityHttpEmitter emitter = new ClarityHttpEmitter(
        config,
        new ClarityNodeDetails(
            TEST_NODE_TYPE,
            config.getClusterName(),
            TEST_DRUID_VERSION,
            TEST_IMPLY_VERSION
        ),
        httpClient,
        JSON_MAPPER
    );
    emitter.start();
    return emitter;
  }

  private UnitEvent buildUnitEvent(String feed, Number value, String metric, String id)
  {
    Map<String, Object> userDims = new HashMap<>();
    userDims.put("metric", metric);
    userDims.put("id", id);

    return new UnitEvent(
        feed,
        value,
        TARGET_URL_KEY,
        TEST_NODE_TYPE,
        TEST_DRUID_VERSION,
        TEST_IMPLY_CLUSTER,
        TEST_IMPLY_VERSION,
        userDims
    );
  }

  private UnitEvent buildUnitEvent(String feed, Number value)
  {
    return buildUnitEvent(feed, value, null, null);
  }

  private UnitEvent buildUnitEvent(Number value)
  {
    return buildUnitEvent("test", value);
  }
}
