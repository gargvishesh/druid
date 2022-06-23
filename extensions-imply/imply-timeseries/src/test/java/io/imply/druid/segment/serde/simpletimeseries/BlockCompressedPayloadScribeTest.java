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
        ByteWriterTestHelper.ValidationFunctionFactory.PAYLOAD_SCRIBE_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, 4115)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, 1049169)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, 1053238)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testEmptyByteArray, 12) // compression header is 12-bytes
            .setReadWriteTestCaseValue(BytesReadWriteTest::testNull, 12) // compression header is 12-bytes
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleLong, 25)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, 1180)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads, 574302)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, 5997)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, 796)
    );
  }
}
