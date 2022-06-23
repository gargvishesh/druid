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
        ByteWriterTestHelper.ValidationFunctionFactory.ROW_READER_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleLong, 62)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testEmptyByteArray, 46)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testNull, 46)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, 4151)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, 1049204)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, 1053277)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, 1655)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, 7368)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads, 575673)
            .setReadWriteTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, 845)
    );
  }
}
