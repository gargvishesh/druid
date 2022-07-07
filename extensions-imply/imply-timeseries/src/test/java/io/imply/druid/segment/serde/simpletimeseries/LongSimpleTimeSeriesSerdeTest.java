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
        new SimpleTimeSeriesSerde(
            new LongDeltaTimestampsEncoderDecoder(
                new LongDeltaEncoderDecoder(SimpleTimeSeriesTestUtil.START_DATE_TIME.getMillis()))),
        new TestCasesConfig<>(SimpleTimeSeriesSerdeTest.class, SimpleTimeSeriesSerdeTestBase.class)
            .enableTestCase(SimpleTimeSeriesSerdeTest::testNull)
            .enableTestCase(SimpleTimeSeriesSerdeTest::testEmptyList)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleItemList, 16) // special case
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testTwoItemList, 44)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testLargerList, 836)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleValueRun, 48)
    );
  }
}
