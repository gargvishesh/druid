/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.SimpleTimeSeries;

import java.nio.ByteBuffer;

public interface SimpleTimeSeriesTestingSerde extends TestingSerde<SimpleTimeSeries>
{
  @Override
  byte[] serialize(SimpleTimeSeries simpleTimeSeries);

  @Override
  SimpleTimeSeries deserialize(ByteBuffer byteBuffer);
}
