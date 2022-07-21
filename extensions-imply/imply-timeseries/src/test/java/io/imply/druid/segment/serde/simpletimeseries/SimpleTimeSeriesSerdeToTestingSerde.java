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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesSerdeToTestingSerde implements SimpleTimeSeriesTestingSerde
{
  private final SimpleTimeSeriesSerde timeSeriesSerde;

  public SimpleTimeSeriesSerdeToTestingSerde(SimpleTimeSeriesSerde timeSeriesSerde)
  {
    this.timeSeriesSerde = timeSeriesSerde;
  }

  @Override
  public byte[] serialize(SimpleTimeSeries simpleTimeSeries)
  {
    return timeSeriesSerde.serialize(simpleTimeSeries);
  }

  @Override
  @Nullable
  public SimpleTimeSeries deserialize(ByteBuffer byteBuffer)
  {
    return timeSeriesSerde.deserialize(byteBuffer);
  }
}
