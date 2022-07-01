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
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, 4115)
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, 1049169)
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, 1053238)
            .setTestCaseValue(BytesReadWriteTest::testEmptyByteArray, 12) // compression header is 12-bytes
            .setTestCaseValue(BytesReadWriteTest::testNull, 12) // compression header is 12-bytes
            .setTestCaseValue(BytesReadWriteTest::testSingleLong, 25)
            .setTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, 1180)
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads, 574302)
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, 5997)
            .setTestCaseValue(BytesReadWriteTest::testSingleUncompressableBlock, 65715)
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, 796)
            .setTestCaseValue(BytesReadWriteTest::testRandomBlockAccess, 3124842)
    );
  }
}
