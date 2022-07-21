/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;

public class BlockCompressedPayloadScribeTest extends BytesReadWriteTestBase
{
  public BlockCompressedPayloadScribeTest()
  {
    super(
        new BlockCompressedPayloadScribeToBytesWriter.Builder(
            new BlockCompressedPayloadScribe.Builder(
                NativeClearedByteBufferProvider.DEFAULT,
                new OnHeapMemorySegmentWriteOutMedium()
            )
        ),
        ByteWriterTestHelper.ValidationFunctionBuilder.PAYLOAD_SCRIBE_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, TestCaseResult.of(4115))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, TestCaseResult.of(1049169))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, TestCaseResult.of(1053238))
            // BytesReadWriteTest::testEmptyByteArray -> compression header is 12-bytes
            .setTestCaseValue(BytesReadWriteTest::testEmptyByteArray, TestCaseResult.of(12))
            .setTestCaseValue(BytesReadWriteTest::testNull, TestCaseResult.of(12)) // compression header is 12-bytes
            .setTestCaseValue(BytesReadWriteTest::testSingleLong, TestCaseResult.of(25))
            .setTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, TestCaseResult.of(1180))
            .setTestCaseValue(
                BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads,
                TestCaseResult.of(574302)
            )
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, TestCaseResult.of(5997))
            .setTestCaseValue(BytesReadWriteTest::testSingleUncompressableBlock, TestCaseResult.of(65715))
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, TestCaseResult.of(796))
            .setTestCaseValue(BytesReadWriteTest::testRandomBlockAccess, TestCaseResult.of(3124842))

    );
  }
}
