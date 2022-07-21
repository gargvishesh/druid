/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import java.nio.ByteBuffer;

/**
 * serde interface for use in tests. Implement adapter from existing prod serde methods to this interface for use
 * in existing test framework
 *
 * eg, SimpleTimeSeriesSerdeTestBase is a set of test cases used to test the integer and long timestamp range
 * serdes for SimpleTimeSeries, as well as SimpleTimeSeriesContainer's methods to/from bytes.
 *
 * @param <T>
 */
public interface TestingSerde<T>
{
  byte[] serialize(T t);

  T deserialize(ByteBuffer byteBuffer);
}
