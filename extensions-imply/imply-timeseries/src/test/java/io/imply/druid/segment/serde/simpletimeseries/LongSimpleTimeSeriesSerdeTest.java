/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

public class LongSimpleTimeSeriesSerdeTest extends SimpleTimeSeriesSerdeTestBase
{
  public LongSimpleTimeSeriesSerdeTest()
  {
    super(
        new SimpleTimeSeriesSerdeToTestingSerde(
            new SimpleTimeSeriesSerde(
                new LongDeltaTimestampsEncoderDecoder(
                    new LongDeltaEncoderDecoder(SimpleTimeSeriesTestUtil.START_DATE_TIME.getMillis())))),
        new TestCasesConfig<>(SimpleTimeSeriesSerdeTest.class, SimpleTimeSeriesSerdeTestBase.class)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testEmptyList, TestCaseResult.of(new byte[0]))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testNull, TestCaseResult.of(new byte[0]))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleItemList, TestCaseResult.of(16)) // special case
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testTwoItemList, TestCaseResult.of(44))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testLargerList, TestCaseResult.of(836))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleValueRun, TestCaseResult.of(48))
    );
  }
}
