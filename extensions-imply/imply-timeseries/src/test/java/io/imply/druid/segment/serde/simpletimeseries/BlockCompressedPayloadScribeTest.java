/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.primitives.Ints;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BlockCompressedPayloadScribeTest
{
  private final SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();

  private ResourceHolder<ByteBuffer> currentBlockHolder;
  private ResourceHolder<ByteBuffer> uncompressedBlockHolder;
  private BlockCompressedPayloadScribe.Factory scribeFactory;

  @Before
  public void setup()
  {
    currentBlockHolder = NativeClearedByteBufferProvider.DEFAULT.get();
    uncompressedBlockHolder = NativeClearedByteBufferProvider.DEFAULT.get();
    scribeFactory = new BlockCompressedPayloadScribe.Factory(new BlockCompressedPayloadWriterFactory(
        NativeClearedByteBufferProvider.DEFAULT,
        writeOutMedium
    ));
  }

  @Test
  public void testSingleWriteBytes() throws Exception
  {
    ByteBuffer sourcePayLoad = generateBufferWithLongs(1024);
    int payloadSize = sourcePayLoad.limit();

    ByteBuffer masterByteBuffer = writePayloadAsByteArray(sourcePayLoad);
    Assert.assertEquals(4115, masterByteBuffer.limit());

    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );
    ByteBuffer readByteBuffer = payloadReader.read(0, payloadSize);

    Assert.assertEquals(sourcePayLoad, readByteBuffer);
  }

  @Test
  public void testSingleMultiBlockWriteBytes() throws Exception
  {
    ByteBuffer payload = generateBufferWithLongs(256 * 1024); // 2mb
    ByteBuffer masterByteBuffer = writePayload(payload, new BufferWriterAsBytes());

    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );

    Assert.assertEquals(payload, payloadReader.read(0, payload.limit()));
  }

  @Test
  public void testSingleMultiBlockWriteBytesWithPrelude() throws Exception
  {
    ByteBuffer payload1 = generateBufferWithLongs(1024); // 8 kb
    ByteBuffer payload2 = generateBufferWithLongs(256 * 1024); // 256kb * 8 = 2mb
    ByteBuffer masterByteBuffer = writePayloadList(Arrays.asList(payload1, payload2), new BufferWriterAsBytes());

    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );

    Assert.assertEquals(payload1, payloadReader.read(0, payload1.limit()));
    Assert.assertEquals(payload2, payloadReader.read(payload1.limit(), payload2.limit()));
  }

  @Test
  public void testEmptyByteArray() throws Exception
  {
    // no-op
    ByteBuffer sourcePayLoad = ByteBuffer.wrap(new byte[0]);
    int payloadSize = sourcePayLoad.limit();
    ByteBuffer masterByteBuffer = writePayloadAsByteArray(sourcePayLoad);
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "1": 4 bytes
    // data stream size : "0" : 4 bytes
    Assert.assertEquals(12, masterByteBuffer.limit());
    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );
    ByteBuffer readByteBuffer = payloadReader.read(0, payloadSize);

    Assert.assertEquals(sourcePayLoad, readByteBuffer);
  }

  @Test
  public void testSingleLong() throws Exception
  {
    ByteBuffer sourcePayLoad = generateBufferWithLongs(1);
    int payloadSize = sourcePayLoad.limit();
    ByteBuffer masterByteBuffer = writePayloadAsByteArray(sourcePayLoad);
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "0": 4 bytes
    // data stream size : "1" : 4 bytes
    // compressed single 8 bytes: 9 bytes (compressed: "0")

    Assert.assertEquals(25, masterByteBuffer.limit());
    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );
    ByteBuffer readByteBuffer = payloadReader.read(0, payloadSize);
    Assert.assertEquals(sourcePayLoad, readByteBuffer);
  }

  @Test
  public void testVariableSizedCompressablePayloads() throws Exception
  {
    List<ByteBuffer> bufferList = generateRaggedPayloadBuffer(100, 1024, 10, 0, 0, 10);
    ByteBuffer masterByteBuffer = validateBufferWriteAndRead(bufferList);
    Assert.assertEquals(1180, masterByteBuffer.limit());
  }

  @Test
  public void testOutliersInNormalDataUncompressablePayloads() throws Exception
  {
    // every integer within a payload is unique
    List<ByteBuffer> bufferList = generateRaggedPayloadBuffer(100, 1024, 10, 64 * 1024, 2);
    ByteBuffer masterByteBuffer = validateBufferWriteAndRead(bufferList);
    Assert.assertEquals(574302, masterByteBuffer.limit());
  }

  @Test
  public void testOutliersInNormalDataCompressablePayloads() throws Exception
  {
    // same # of payloads and size of payloads as testOutliersInNormalDataUncompressablePayloads()
    // integer values range 0-9
    List<ByteBuffer> bufferList = generateRaggedPayloadBuffer(100, 1024, 10, 64 * 1024, 2, 10);
    ByteBuffer masterByteBuffer = validateBufferWriteAndRead(bufferList);
    Assert.assertEquals(5997, masterByteBuffer.limit());
  }

  private ByteBuffer validateBufferWriteAndRead(List<ByteBuffer> bufferList) throws IOException
  {
    ByteBuffer masterByteBuffer = writePayloadList(bufferList, new BufferWriterAsBytes());
    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );
    int position = 0;

    for (int i = 0; i < bufferList.size(); i++) {
      ByteBuffer expectedByteBuffer = bufferList.get(i);

      ByteBuffer readByteBuffer = payloadReader.read(position, expectedByteBuffer.limit());
      position += expectedByteBuffer.limit();
      Assert.assertEquals(StringUtils.format("failure on buffer %s", i), expectedByteBuffer, readByteBuffer);
    }

    return masterByteBuffer;
  }

  @Test
  public void testSingleWriteByteBuffer() throws Exception
  {
    ByteBuffer sourcePayLoad = generateBufferWithLongs(1024); // 8k
    int payloadSize = sourcePayLoad.limit();
    ByteBuffer masterByteBuffer = writePayloadAsByteBuffer(sourcePayLoad);

    Assert.assertEquals(4115, masterByteBuffer.limit());
    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlockHolder.get()
    );
    ByteBuffer readByteBuffer = payloadReader.read(0, payloadSize);

    Assert.assertEquals(sourcePayLoad, readByteBuffer);
  }

  @Nonnull
  private ByteBuffer writePayloadAsByteArray(ByteBuffer payload) throws IOException
  {
    return writePayload(payload, BufferWriterAsBytes.INSTANCE);
  }

  private ByteBuffer writePayloadAsByteBuffer(ByteBuffer payload) throws IOException
  {
    return writePayload(payload, BufferWriterAsBuffer.INSTANCE);
  }

  private static List<ByteBuffer> generateRaggedPayloadBuffer(
      int baseMin,
      int baseMax,
      int stepSize,
      int largeSize,
      int largeCount
  )
  {
    return generateRaggedPayloadBuffer(baseMin, baseMax, stepSize, largeSize, largeCount, Integer.MAX_VALUE);
  }

  private static List<ByteBuffer> generateRaggedPayloadBuffer(
      int baseMin,
      int baseMax,
      int stepSize,
      int largeSize,
      int largeCount,
      int modulo
  )
  {
    List<ByteBuffer> byteBufferList = new ArrayList<>();

    for (int i = baseMin; i < baseMax; i += stepSize) {
      byteBufferList.add(generateIntPayloads(baseMin + i, modulo));
    }

    for (int j = 0; j < largeCount; j++) {
      byteBufferList.add(generateIntPayloads(largeSize, modulo));

      for (int i = baseMin; i < baseMax; i += stepSize) {
        byteBufferList.add(generateIntPayloads(baseMin + i, modulo));
      }
    }

    return byteBufferList;
  }

  private ByteBuffer writePayload(ByteBuffer sourcePayLoad, BufferWriter bufferWriter) throws IOException
  {
    return writePayloadList(Collections.singletonList(sourcePayLoad), bufferWriter);
  }

  private ByteBuffer writePayloadList(List<ByteBuffer> payloadList) throws IOException
  {
    return writePayloadList(payloadList, BufferWriterAsBytes.INSTANCE);

  }

  private ByteBuffer writePayloadList(List<ByteBuffer> payloadList, BufferWriter bufferWriter) throws IOException
  {
    BlockCompressedPayloadScribe payloadScribe = scribeFactory.create();

    for (ByteBuffer payload : payloadList) {
      bufferWriter.writeTo(payloadScribe, payload);
    }

    payloadScribe.close();

    HeapByteBufferWriteOutBytes bufferWriteOutBytes = new HeapByteBufferWriteOutBytes();

    payloadScribe.transferTo(bufferWriteOutBytes);

    int payloadSerializedSize = Ints.checkedCast(payloadScribe.getSerializedSize());
    ByteBuffer masterByteBuffer = ByteBuffer.allocate(payloadSerializedSize).order(ByteOrder.nativeOrder());

    bufferWriteOutBytes.readFully(0, masterByteBuffer);
    masterByteBuffer.flip();

    Assert.assertEquals(payloadScribe.getSerializedSize(), masterByteBuffer.limit());

    return masterByteBuffer;
  }

  private static ByteBuffer generateIntPayloads(int intCount)
  {
    return generateIntPayloads(intCount, Integer.MAX_VALUE);
  }

  private static ByteBuffer generateIntPayloads(int intCount, int modulo)
  {
    ByteBuffer payload = ByteBuffer.allocate(Integer.BYTES * intCount).order(ByteOrder.nativeOrder());

    for (int i = intCount - 1; i >= 0; i--) {

      payload.putInt(i % modulo);
    }

    payload.flip();

    return payload;
  }

  @Nonnull
  private static ByteBuffer generateBufferWithLongs(int longCount)
  {
    ByteBuffer longPayload = ByteBuffer.allocate(Long.BYTES * longCount).order(ByteOrder.nativeOrder());

    for (int i = 0; i < longCount; i++) {
      longPayload.putLong(longCount - i - 1);
    }

    longPayload.flip();

    return longPayload;
  }

  @After
  public void tearDown()
  {
    currentBlockHolder.close();
    uncompressedBlockHolder.close();
  }

  private interface BufferWriter
  {
    void writeTo(BlockCompressedPayloadScribe scribe, ByteBuffer payload) throws IOException;
  }

  private static class BufferWriterAsBytes implements BufferWriter
  {
    private static final BufferWriterAsBytes INSTANCE = new BufferWriterAsBytes();

    @Override
    public void writeTo(BlockCompressedPayloadScribe scribe, ByteBuffer payload) throws IOException
    {
      scribe.write(payload.array());
    }
  }

  private static class BufferWriterAsBuffer implements BufferWriter
  {
    private static final BufferWriterAsBuffer INSTANCE = new BufferWriterAsBuffer();

    @Override
    public void writeTo(BlockCompressedPayloadScribe scribe, ByteBuffer payload) throws IOException
    {
      scribe.write(payload);
    }
  }
}
