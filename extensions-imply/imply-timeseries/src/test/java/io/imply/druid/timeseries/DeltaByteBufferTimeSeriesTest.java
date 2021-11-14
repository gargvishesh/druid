/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

import java.nio.ByteBuffer;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public class DeltaByteBufferTimeSeriesTest extends DeltaTimeSeriesBaseTest
{
  @Override
  public SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window, DurationGranularity durationGranularity)
  {
    WritableMemory mem = WritableMemory.writableWrap(ByteBuffer.allocateDirect(1200)); // simulate the real deal
    WritableMemory finalMem = WritableMemory.writableWrap(ByteBuffer.allocateDirect(1200));
    int buffStartPosition = 0;

    DeltaTimeSeries[] seriesToMerge = new DeltaTimeSeries[seriesList.length];
    DeltaByteBufferTimeSeries[] bufferSeriesList = new DeltaByteBufferTimeSeries[seriesList.length];
    for (int i = 0; i < seriesList.length; i++) {
      bufferSeriesList[i] = new DeltaByteBufferTimeSeries(durationGranularity, window, MAX_ENTRIES);
      bufferSeriesList[i].init(mem, buffStartPosition);
      bufferSeriesList[i].setStartBuffered(mem, buffStartPosition, seriesList[i].getStart());
      bufferSeriesList[i].setEndBuffered(mem, buffStartPosition, seriesList[i].getEnd());
      for (int j = 0; j < seriesList[i].size(); j++) {
        bufferSeriesList[i].addDataPointBuffered(mem,
                                                 buffStartPosition,
                                                 seriesList[i].getTimestamps().getLong(j),
                                                 seriesList[i].getDataPoints().getDouble(j));
      }
      seriesToMerge[i] = bufferSeriesList[i].computeDeltaBuffered(mem, buffStartPosition);
    }

    DeltaByteBufferTimeSeries timeSeries = new DeltaByteBufferTimeSeries(durationGranularity, window, MAX_ENTRIES);
    timeSeries.init(finalMem, buffStartPosition);
    for (DeltaTimeSeries deltaTimeSeries : seriesToMerge) {
      timeSeries.mergeSeriesBuffered(finalMem, buffStartPosition, deltaTimeSeries);
    }

    return timeSeries.computeSimpleBuffered(finalMem, buffStartPosition);
  }
}
