/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.channel.ReadableByteChunksFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.testutil.FrameSequenceBuilder;
import io.imply.druid.talaria.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@RunWith(Parameterized.class)
public class FrameFileHttpResponseHandlerTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final int maxRowsPerFrame;
  private final BlockingQueue<Throwable> caughtExceptions = new LinkedBlockingDeque<>();

  private StorageAdapter adapter;
  private File file;
  private ReadableByteChunksFrameChannel channel;
  private FrameFileHttpResponseHandler handler;
  private HttpResponseHandler.TrafficCop trafficCop;

  public FrameFileHttpResponseHandlerTest(final int maxRowsPerFrame)
  {
    this.maxRowsPerFrame = maxRowsPerFrame;
  }

  @Parameterized.Parameters(name = "maxRowsPerFrame = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (int maxRowsPerFrame : new int[]{1, 50, Integer.MAX_VALUE}) {
      constructors.add(new Object[]{maxRowsPerFrame});
    }

    return constructors;
  }

  @Before
  public void setUp() throws IOException
  {
    adapter = new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
    file = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .maxRowsPerFrame(maxRowsPerFrame)
                            .frameType(FrameType.ROW_BASED) // No particular reason to test with both frame types
                            .frames(),
        temporaryFolder.newFile()
    );

    channel = ReadableByteChunksFrameChannel.create("test");
    handler = new FrameFileHttpResponseHandler(channel, caughtExceptions::add);
    trafficCop = EasyMock.createMock(HttpResponseHandler.TrafficCop.class);
  }

  @After
  public void tearDown()
  {
    // Tests should clear these out if they expected exceptions.
    Assert.assertEquals("number of exceptions", 0, caughtExceptions.size());
    EasyMock.verify(trafficCop);
  }

  @Test
  public void testNonChunkedResponse() throws Exception
  {
    EasyMock.expect(trafficCop.resume(0)).andReturn(0L);
    EasyMock.replay(trafficCop);

    final ClientResponse<ReadableByteChunksFrameChannel> response1 = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, Files.readAllBytes(file.toPath())),
        trafficCop
    );

    Assert.assertTrue(response1.isFinished());
    Assert.assertFalse(response1.isContinueReading());

    final ClientResponse<ReadableFrameChannel> response2 = handler.done(response1);

    Assert.assertTrue(response2.isFinished());
    Assert.assertTrue(response2.isContinueReading());

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
    );
  }

  @Test
  public void testChunkedResponse() throws Exception
  {
    EasyMock.expect(trafficCop.resume(EasyMock.anyLong())).andReturn(0L).atLeastOnce();
    EasyMock.replay(trafficCop);

    final int chunkSize = 99;
    final byte[] allBytes = Files.readAllBytes(file.toPath());

    ClientResponse<ReadableByteChunksFrameChannel> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, byteSlice(allBytes, 0, chunkSize)),
        trafficCop
    );

    Assert.assertTrue(response.isFinished());

    for (int p = chunkSize; p < allBytes.length; p += chunkSize) {
      response = handler.handleChunk(
          response,
          makeChunk(byteSlice(allBytes, p, chunkSize)),
          p / chunkSize
      );

      Assert.assertTrue(response.isFinished());
    }

    final ClientResponse<ReadableFrameChannel> finalResponse = handler.done(response);

    Assert.assertTrue(finalResponse.isFinished());
    Assert.assertTrue(finalResponse.isContinueReading());

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
    );
  }

  @Test
  public void testServerErrorResponse()
  {
    EasyMock.replay(trafficCop);

    ClientResponse<ReadableByteChunksFrameChannel> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, StringUtils.toUtf8("Oh no!")),
        trafficCop
    );

    final ClientResponse<ReadableFrameChannel> finalResponse = handler.done(response);
    Assert.assertTrue(finalResponse.isFinished());
    Assert.assertTrue(finalResponse.isContinueReading());

    // Verify that the exception handler was called.
    final Throwable e = caughtExceptions.poll();
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.equalTo("Server for [test] returned [500 Internal Server Error]")
        )
    );

    // Verify that the channel has not had an error state set. This enables reconnection later.
    Assert.assertFalse(channel.isErrorOrFinished());
  }

  @Test
  public void testChunkedServerErrorResponse()
  {
    EasyMock.replay(trafficCop);

    ClientResponse<ReadableByteChunksFrameChannel> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, StringUtils.toUtf8("Oh ")),
        trafficCop
    );

    response = handler.handleChunk(response, makeChunk(StringUtils.toUtf8("no!")), 1);

    final ClientResponse<ReadableFrameChannel> finalResponse = handler.done(response);
    Assert.assertTrue(finalResponse.isFinished());
    Assert.assertTrue(finalResponse.isContinueReading());

    // Verify that the exception handler was called.
    final Throwable e = caughtExceptions.poll();
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.equalTo("Server for [test] returned [500 Internal Server Error]")
        )
    );

    // Verify that the channel has not had an error state set. This enables reconnection later.
    Assert.assertFalse(channel.isErrorOrFinished());
  }

  @Test
  public void testCaughtExceptionDuringChunkedResponse() throws Exception
  {
    EasyMock.expect(trafficCop.resume(EasyMock.anyLong())).andReturn(0L).anyTimes();
    EasyMock.replay(trafficCop);

    // Split file into 4 quarters.
    final int chunkSize = Ints.checkedCast(LongMath.divide(file.length(), 4, RoundingMode.CEILING));
    final byte[] allBytes = Files.readAllBytes(file.toPath());

    ClientResponse<ReadableByteChunksFrameChannel> response = handler.handleResponse(
        makeResponse(HttpResponseStatus.OK, byteSlice(allBytes, 0, chunkSize)),
        trafficCop
    );

    Assert.assertTrue(response.isFinished());

    // Add next chunk.
    response = handler.handleChunk(
        response,
        makeChunk(byteSlice(allBytes, chunkSize, chunkSize)),
        1
    );

    // Set an exception.
    handler.exceptionCaught(response, new ISE("Oh no!"));

    // Add another chunk after the exception is caught (this can happen in real life!). We expect it to be ignored.
    handler.handleChunk(
        response,
        makeChunk(byteSlice(allBytes, chunkSize * 2, chunkSize)),
        2
    );

    // Verify that the exception handler was called.
    final Throwable e = caughtExceptions.poll();
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("Oh no!")));

    // Verify that the channel has not had an error state set.
    Assert.assertFalse(channel.isErrorOrFinished());

    // Simulate successful reconnection with a different handler: add the rest of the data directly to the channel.
    channel.addChunk(byteSlice(allBytes, chunkSize * 2, chunkSize));
    channel.addChunk(byteSlice(allBytes, chunkSize * 3, chunkSize));
    Assert.assertEquals(allBytes.length, channel.getBytesAdded());
    channel.doneWriting();

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
    );
  }

  private static HttpResponse makeResponse(final HttpResponseStatus status, final byte[] content)
  {
    final ByteBufferBackedChannelBuffer channelBuffer = new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(content));

    return new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    {
      @Override
      public ChannelBuffer getContent()
      {
        return channelBuffer;
      }
    };
  }

  private static HttpChunk makeChunk(final byte[] content)
  {
    final ByteBufferBackedChannelBuffer channelBuffer = new ByteBufferBackedChannelBuffer(ByteBuffer.wrap(content));
    return new DefaultHttpChunk(channelBuffer);
  }

  private static byte[] byteSlice(final byte[] bytes, final int start, final int length)
  {
    final int actualLength = Math.min(bytes.length - start, length);
    final byte[] retVal = new byte[actualLength];
    System.arraycopy(bytes, start, retVal, 0, actualLength);
    return retVal;
  }
}
