/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

public class IntegerSimpleTimeSeriesSerdeTest extends SimpleTimeSeriesSerdeTestBase
{
  public IntegerSimpleTimeSeriesSerdeTest()
  {
    super(
        new SimpleTimeSeriesSerdeToTestingSerde(
            new SimpleTimeSeriesSerde(
                new IntegerDeltaTimestampsEncoderDecoder(
                    new IntegerDeltaEncoderDecoder(SimpleTimeSeriesTestUtil.START_DATE_TIME.getMillis())))),
        new TestCasesConfig<>(SimpleTimeSeriesSerdeTest.class, SimpleTimeSeriesSerdeTestBase.class)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testEmptyList, TestCaseResult.of(new byte[0]))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testNull, TestCaseResult.of(new byte[0]))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleItemList, TestCaseResult.of(16))
            // for the testTwoItemList test, the delta is small enough to fit in integers, so 4 byte header + 12 bytes
            // for timestamps + 20 bytes for values (timestamps and deltas have int for list size since due to rle, each
            // list may vary in size)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testTwoItemList, TestCaseResult.of(36))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testLargerList, TestCaseResult.of(828))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleValueRun, TestCaseResult.of(40))
    );
  }
}
