/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuppressWarnings({"UnconstructableJUnitTestCase", "NewClassNamingConvention"})
@Ignore
public class BytesReadWriteTestBase implements BytesReadWriteTest
{
  protected final BytesWriterBuilder bytesWriterBuilder;

  private final BytesReadWriteTestCases testCases;
  private final ByteWriterTestHelper.ValidationFunctionFactory validationFunctionFactory;

  private ByteWriterTestHelper testHelper;
  private Closer closer;

  protected BytesReadWriteTestBase(
      BytesWriterBuilder bytesWriterBuilder,
      ByteWriterTestHelper.ValidationFunctionFactory validationFunctionFactory,
      BytesReadWriteTestCases testCases
  )
  {
    this.testCases = testCases;
    this.bytesWriterBuilder = bytesWriterBuilder;
    this.validationFunctionFactory = validationFunctionFactory;
  }

  @Before
  public void setup()
  {
    ResourceHolder<ByteBuffer> uncompressedBlockHolder1 = NativeClearedByteBufferProvider.DEFAULT.get();
    ResourceHolder<ByteBuffer> uncompressedBlockHolder2 = NativeClearedByteBufferProvider.DEFAULT.get();
    ResourceHolder<ByteBuffer> currentBlockHolder = NativeClearedByteBufferProvider.DEFAULT.get();

    closer = Closer.create();
    closer.register(uncompressedBlockHolder1);
    closer.register(uncompressedBlockHolder2);
    closer.register(currentBlockHolder);
    testHelper = new ByteWriterTestHelper(
        bytesWriterBuilder,
        uncompressedBlockHolder1.get(),
        uncompressedBlockHolder2.get(),
        validationFunctionFactory
    );
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Test
  @Override
  public void testSingleWriteBytes() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    ByteBuffer payload = testHelper.generateBufferWithLongs(1024);
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Collections.singletonList(payload), expectedSize);
  }

  @Test
  @Override
  public void testSingleMultiBlockWriteBytes() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    ByteBuffer payload = testHelper.generateBufferWithLongs(256 * 1024); // 2mb
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Collections.singletonList(payload), expectedSize);
  }

  @Test
  @Override
  public void testSingleMultiBlockWriteBytesWithPrelude() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    ByteBuffer payload1 = testHelper.generateBufferWithLongs(1024); // 8 kb
    ByteBuffer payload2 = testHelper.generateBufferWithLongs(256 * 1024); // 256kb * 8 = 2mb
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Arrays.asList(payload1, payload2), expectedSize);
  }

  @Test
  @Override
  public void testEmptyByteArray() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    // no-op
    ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "1": 4 bytes
    // data stream size : "0" : 4 bytes
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Collections.singletonList(payload), expectedSize);
  }


  @Test
  @Override
  public void testNull() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    // no-op
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "1": 4 bytes
    // data stream size : "0" : 4 bytes
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Collections.singletonList(null), expectedSize);
  }

  @Test
  @Override
  public void testSingleLong() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    ByteBuffer payload = testHelper.generateBufferWithLongs(1);
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "0": 4 bytes
    // data stream size : "1" : 4 bytes
    // compressed single 8 bytes: 9 bytes (compressed: "0")
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Collections.singletonList(payload), expectedSize);
  }

  @Test
  @Override
  public void testVariableSizedCompressablePayloads() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 0, 0, 10);
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(bufferList, expectedSize);
  }

  @Test
  @Override
  public void testOutliersInNormalDataUncompressablePayloads() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    // every integer within a payload is unique
    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 64 * 1024, 2);
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(bufferList, expectedSize);
  }

  @Test
  @Override
  public void testOutliersInNormalDataCompressablePayloads() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    // same # of payloads and size of payloads as testOutliersInNormalDataUncompressablePayloads()
    // integer values range 0-9
    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 64 * 1024, 2, 10);
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(bufferList, expectedSize);
  }

  @Test
  @Override
  public void testSingleWriteByteBufferZSTD() throws Exception
  {
    Assume.assumeTrue(testCases.currentTestEnabled());
    ByteBuffer sourcePayLoad = testHelper.generateBufferWithLongs(1024); // 8k
    testHelper.setCompressionStrategy(CompressionStrategy.ZSTD);
    int expectedSize = testCases.currentTestValue();
    testHelper.validateReadAndSize(Collections.singletonList(sourcePayLoad), expectedSize);
  }
}
