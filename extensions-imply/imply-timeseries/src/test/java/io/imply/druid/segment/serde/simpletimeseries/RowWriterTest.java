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

public class RowWriterTest extends BytesReadWriteTestBase
{
  public RowWriterTest()
  {
    super(
        new RowWriterToBytesWriter.Builder(
            new RowWriter.Builder(NativeClearedByteBufferProvider.DEFAULT, new OnHeapMemorySegmentWriteOutMedium())
        ),
        ByteWriterTestHelper.ValidationFunctionBuilder.ROW_READER_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setTestCaseValue(BytesReadWriteTest::testSingleLong, 62)
            .setTestCaseValue(BytesReadWriteTest::testEmptyByteArray, 46)
            .setTestCaseValue(BytesReadWriteTest::testNull, 46)
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, 4151)
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, 1049204)
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, 1053277)
            .setTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, 1655)
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, 7368)
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads, 575673)
            .setTestCaseValue(BytesReadWriteTest::testSingleUncompressableBlock, 65750)
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, 845)
            .setTestCaseValue(BytesReadWriteTest::testRandomBlockAccess, 3126618)
    );
  }
}
