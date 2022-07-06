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
import java.util.Random;

public class ByteWriterTestHelper
{
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final BytesWriterBuilder bytesWriterBuilder;
  private final ByteBuffer uncompressedBlock1;
  private final ByteBuffer uncompressedBlock2;
  private final ValidationFunctionBuilder validationFunctionBuilder;
  private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;

  public ByteWriterTestHelper(
      BytesWriterBuilder bytesWriterBuilder,
      ByteBuffer uncompressedBlock1,
      ByteBuffer uncompressedBlock2,
      ValidationFunctionBuilder validationFunctionBuilder
  )
  {
    this.bytesWriterBuilder = bytesWriterBuilder;
    this.uncompressedBlock1 = uncompressedBlock1;
    this.uncompressedBlock2 = uncompressedBlock2;
    this.validationFunctionBuilder = validationFunctionBuilder;
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
    ValidationFunction validationFunction = validationFunctionBuilder.create(this);
    validationFunction.validateBufferList(byteBufferList);
  }

  public void validateReadAndSize(List<ByteBuffer> byteBufferList, int expectedSize) throws Exception
  {
    ValidationFunction validationFunction = validationFunctionBuilder.create(this);
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

  public ByteBuffer validateBufferWriteAndReadBlockCompressedScribe(List<ByteBuffer> bufferList, boolean useRandom)
      throws IOException
  {
    long position = 0;
    List<PayloadEntrySpan> payloadReadList = new ArrayList<>();

    for (ByteBuffer byteBuffer : bufferList) {
      int expectedSize = byteBuffer == null ? 0 : byteBuffer.limit();
      payloadReadList.add(new PayloadEntrySpan(position, expectedSize));
      position += expectedSize;
    }

    ByteBuffer masterByteBuffer = writePayloadList(bufferList, new BufferWriterAsBytes());
    BlockCompressedPayloadReader payloadReader = BlockCompressedPayloadReader.create(
        masterByteBuffer,
        uncompressedBlock1,
        compressionStrategy.getDecompressor()
    );
    List<Integer> positions = new ArrayList<>(bufferList.size());

    for (int i = 0; i < bufferList.size(); i++) {
      positions.add(i);
    }

    Random random = new Random(0);

    if (useRandom) {
      Collections.shuffle(positions, random);
    }

    for (int index : positions) {
      ByteBuffer expectedByteBuffer = bufferList.get(index);
      PayloadEntrySpan payloadEntrySpan = payloadReadList.get(index);
      ByteBuffer readByteBuffer = payloadReader.read(payloadEntrySpan.getStart(), payloadEntrySpan.getSize());

      if (expectedByteBuffer == null) {
        Assert.assertEquals(StringUtils.format("expected empty buffer %s", index), EMPTY_BYTE_BUFFER, readByteBuffer);
      } else {
        Assert.assertEquals(StringUtils.format("failure on buffer %s", index), expectedByteBuffer, readByteBuffer);
      }
    }

    return masterByteBuffer;
  }

  public ByteBuffer validateBufferWriteAndReadRow(List<ByteBuffer> bufferList, boolean useRandomRead) throws IOException
  {
    ByteBuffer masterByteBuffer = writePayloadList(bufferList, new BufferWriterAsBytes());
    RowReader rowReader = new RowReader.Builder(masterByteBuffer)
        .setCompressionStrategy(compressionStrategy)
        .build(uncompressedBlock1, uncompressedBlock2);

    List<Integer> positions = new ArrayList<>(bufferList.size());

    for (int i = 0; i < bufferList.size(); i++) {
      positions.add(i);
    }

    Random random = new Random(0);

    if (useRandomRead) {
      Collections.shuffle(positions, random);
    }


    for (int index : positions) {
      ByteBuffer expectedByteBuffer = bufferList.get(index);

      ByteBuffer readByteBuffer = rowReader.getRow(index);
      if (expectedByteBuffer == null) {
        Assert.assertEquals(StringUtils.format("failure on buffer %s", index), 0L, readByteBuffer.remaining());
      } else {
        Assert.assertEquals(StringUtils.format("failure on buffer %s", index), expectedByteBuffer, readByteBuffer);
      }
    }

    return masterByteBuffer;
  }

  public ByteWriterTestHelper setUseRandomReadOrder(boolean useReadRandom)
  {
    validationFunctionBuilder.setReadRandom(useReadRandom);

    return this;
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

  public interface ValidationFunctionBuilder
  {
    ValidationFunctionBuilder PAYLOAD_SCRIBE_VALIDATION_FUNCTION_FACTORY = new PayloadScribeValidationFunctionBuilder();

    ValidationFunctionBuilder ROW_READER_VALIDATION_FUNCTION_FACTORY = new RowReaderValidationFunctionBuilder();

    ValidationFunction create(ByteWriterTestHelper testHelper);

    ValidationFunctionBuilder setReadRandom(boolean useRandomRead);
  }

  public static class PayloadScribeValidationFunctionBuilder implements ValidationFunctionBuilder
  {
    private boolean useRandomRead;

    @Override
    public ValidationFunction create(ByteWriterTestHelper testHelper)
    {
      return bufferList -> testHelper.validateBufferWriteAndReadBlockCompressedScribe(bufferList, useRandomRead);
    }

    @Override
    public ValidationFunctionBuilder setReadRandom(boolean useRandomRead)
    {
      this.useRandomRead = useRandomRead;
      return this;
    }
  }

  public static class RowReaderValidationFunctionBuilder implements ValidationFunctionBuilder
  {
    private boolean useRandomRead;

    @Override
    public ValidationFunction create(ByteWriterTestHelper testHelper)
    {
      return bufferList -> testHelper.validateBufferWriteAndReadRow(bufferList, useRandomRead);
    }

    @Override
    public ValidationFunctionBuilder setReadRandom(boolean useRandomRead)
    {
      this.useRandomRead = useRandomRead;
      return this;
    }
  }
}
