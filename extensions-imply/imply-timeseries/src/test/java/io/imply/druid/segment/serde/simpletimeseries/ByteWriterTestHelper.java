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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.junit.Assert;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ByteWriterTestHelper
{
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final BytesWriterBuilder bytesWriterBuilder;
  private final ByteBuffer uncompressedBlock1;
  private final ByteBuffer uncompressedBlock2;
  private final ValidationFunctionFactory validationFunctionFactory;
  private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;

  public ByteWriterTestHelper(
      BytesWriterBuilder bytesWriterBuilder,
      ByteBuffer uncompressedBlock1,
      ByteBuffer uncompressedBlock2,
      ValidationFunctionFactory validationFunctionFactory
  )
  {
    this.bytesWriterBuilder = bytesWriterBuilder;
    this.uncompressedBlock1 = uncompressedBlock1;
    this.uncompressedBlock2 = uncompressedBlock2;
    this.validationFunctionFactory = validationFunctionFactory;
  }

  public ByteWriterTestHelper setCompressionStrategy(CompressionStrategy compressionStrategy)
  {
    this.compressionStrategy = compressionStrategy;
    bytesWriterBuilder.setCompressionStrategy(compressionStrategy);

    return this;
  }

  public ByteBuffer writePayloadAsByteArray(ByteBuffer payload) throws IOException
  {
    return writePayload(payload, BufferWriterAsBytes.INSTANCE);
  }

  public ByteBuffer writePayloadAsByteBuffer(ByteBuffer payload) throws IOException
  {
    return writePayload(payload, BufferWriterAsBuffer.INSTANCE);
  }

  public List<ByteBuffer> generateRaggedPayloadBuffer(
      int baseMin,
      int baseMax,
      int stepSize,
      int largeSize,
      int largeCount
  )
  {
    return generateRaggedPayloadBuffer(baseMin, baseMax, stepSize, largeSize, largeCount, Integer.MAX_VALUE);
  }

  public List<ByteBuffer> generateRaggedPayloadBuffer(
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

  public void validateRead(List<ByteBuffer> byteBufferList) throws Exception
  {
    ValidationFunction validationFunction = validationFunctionFactory.create(this);
    validationFunction.validateBufferList(byteBufferList);
  }

  public void validateReadAndSize(List<ByteBuffer> byteBufferList, int expectedSize) throws Exception
  {
    ValidationFunction validationFunction = validationFunctionFactory.create(this);
    ByteBuffer masterByteBuffer = validationFunction.validateBufferList(byteBufferList);
    int actualSize = masterByteBuffer.limit();

    if (expectedSize > -1) {
      Assert.assertEquals(expectedSize, actualSize);
    }
  }

  public ByteBuffer writePayload(ByteBuffer sourcePayLoad, BufferWriter bufferWriter) throws IOException
  {
    return writePayloadList(Collections.singletonList(sourcePayLoad), bufferWriter);
  }

  public ByteBuffer writePayloadList(List<ByteBuffer> payloadList) throws IOException
  {
    return writePayloadList(payloadList, BufferWriterAsBuffer.INSTANCE);

  }

  public ByteBuffer writePayloadList(List<ByteBuffer> payloadList, BufferWriter bufferWriter) throws IOException
  {
    BytesWriter bytesWriter = bytesWriterBuilder.build();

    for (ByteBuffer payload : payloadList) {
      bufferWriter.writeTo(bytesWriter, payload);
    }

    bytesWriter.close();

    HeapByteBufferWriteOutBytes bufferWriteOutBytes = new HeapByteBufferWriteOutBytes();

    bytesWriter.transferTo(bufferWriteOutBytes);

    int payloadSerializedSize = Ints.checkedCast(bytesWriter.getSerializedSize());
    ByteBuffer masterByteBuffer = ByteBuffer.allocate(payloadSerializedSize).order(ByteOrder.nativeOrder());

    bufferWriteOutBytes.readFully(0, masterByteBuffer);
    masterByteBuffer.flip();

    Assert.assertEquals(bytesWriter.getSerializedSize(), masterByteBuffer.limit());

    return masterByteBuffer;
  }

  public ByteBuffer generateIntPayloads(int intCount)
  {
    return generateIntPayloads(intCount, Integer.MAX_VALUE);
  }

  public ByteBuffer generateIntPayloads(int intCount, int modulo)
  {
    ByteBuffer payload = ByteBuffer.allocate(Integer.BYTES * intCount).order(ByteOrder.nativeOrder());

    for (int i = intCount - 1; i >= 0; i--) {

      payload.putInt(i % modulo);
    }

    payload.flip();

    return payload;
  }

  @Nonnull
  public ByteBuffer generateBufferWithLongs(int longCount)
  {
    ByteBuffer longPayload = ByteBuffer.allocate(Long.BYTES * longCount).order(ByteOrder.nativeOrder());

    for (int i = 0; i < longCount; i++) {
      longPayload.putLong(longCount - i - 1);
    }

    longPayload.flip();

    return longPayload;
  }

  public ByteBuffer validateBufferWriteAndReadBlockCompressedScribe(List<ByteBuffer> bufferList) throws IOException
  {
    ByteBuffer masterByteBuffer = writePayloadList(bufferList, new BufferWriterAsBytes());
    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlock1,
        compressionStrategy.getDecompressor()
    );
    int position = 0;

    for (int i = 0; i < bufferList.size(); i++) {
      ByteBuffer expectedByteBuffer = bufferList.get(i);
      int expectedSize = expectedByteBuffer == null ? 0 : expectedByteBuffer.limit();

      ByteBuffer readByteBuffer = payloadReader.read(position, expectedSize);
      position += expectedSize;
      if (expectedByteBuffer == null) {
        Assert.assertEquals(StringUtils.format("expected empty buffer %s", i), EMPTY_BYTE_BUFFER, readByteBuffer);
      } else {
        Assert.assertEquals(StringUtils.format("failure on buffer %s", i), expectedByteBuffer, readByteBuffer);
      }
    }

    return masterByteBuffer;
  }

  public ByteBuffer validateBufferWriteAndReadRow(List<ByteBuffer> bufferList) throws IOException
  {
    ByteBuffer masterByteBuffer = writePayloadList(bufferList, new BufferWriterAsBytes());
    RowReader rowReader = new RowReader.Builder(masterByteBuffer)
        .setCompressionStrategy(compressionStrategy)
        .build(uncompressedBlock1, uncompressedBlock2);

    for (int i = 0; i < bufferList.size(); i++) {
      ByteBuffer expectedByteBuffer = bufferList.get(i);

      ByteBuffer readByteBuffer = rowReader.getRow(i);
      if (expectedByteBuffer == null) {
        Assert.assertEquals(StringUtils.format("failure on buffer %s", i), 0L, readByteBuffer.remaining());
      } else {
        Assert.assertEquals(StringUtils.format("failure on buffer %s", i), expectedByteBuffer, readByteBuffer);
      }
    }

    return masterByteBuffer;
  }

  public interface BufferWriter
  {
    void writeTo(BytesWriter scribe, ByteBuffer payload) throws IOException;
  }

  public static class BufferWriterAsBytes implements BufferWriter
  {
    public static final BufferWriterAsBytes INSTANCE = new BufferWriterAsBytes();

    @Override
    public void writeTo(BytesWriter scribe, ByteBuffer payload) throws IOException
    {
      scribe.write(payload);
    }
  }

  public static class BufferWriterAsBuffer implements BufferWriter
  {
    public static final BufferWriterAsBuffer INSTANCE = new BufferWriterAsBuffer();

    @Override
    public void writeTo(BytesWriter scribe, ByteBuffer payload) throws IOException
    {
      scribe.write(payload);
    }
  }

  public interface ValidationFunction
  {
    ByteBuffer validateBufferList(List<ByteBuffer> byteBufferList) throws Exception;
  }

  public interface ValidationFunctionFactory
  {
    ValidationFunctionFactory PAYLOAD_SCRIBE_VALIDATION_FUNCTION_FACTORY =
        testHelper -> testHelper::validateBufferWriteAndReadBlockCompressedScribe;

    ValidationFunctionFactory ROW_READER_VALIDATION_FUNCTION_FACTORY =
        testHelper -> testHelper::validateBufferWriteAndReadRow;

    ValidationFunction create(ByteWriterTestHelper testHelper);
  }
}
