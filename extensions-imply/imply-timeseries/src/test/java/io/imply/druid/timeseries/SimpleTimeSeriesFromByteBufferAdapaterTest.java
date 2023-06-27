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
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;

import java.nio.ByteBuffer;

public class SimpleTimeSeriesFromByteBufferAdapaterTest extends SimpleTimeSeriesBaseTest
{
  public static final Interval VISIBLE_WINDOW = Intervals.utc(0, 1000);

  @Override
  public SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window)
  {
    WritableMemory mem = WritableMemory.writableWrap(ByteBuffer.allocateDirect(600)); // simulate the real deal
    WritableMemory finalMem = WritableMemory.writableWrap(ByteBuffer.allocateDirect(600));
    int buffStartPosition = 0;
    SimpleTimeSeriesFromByteBufferAdapter timeSeries = new SimpleTimeSeriesFromByteBufferAdapter(window, MAX_ENTRIES);
    timeSeries.init(finalMem, buffStartPosition);

    SimpleTimeSeries[] seriesToMerge = new SimpleTimeSeries[seriesList.length];
    SimpleTimeSeriesFromByteBufferAdapter[] bufferSeriesList = new SimpleTimeSeriesFromByteBufferAdapter[seriesList.length];
    for (int i = 0; i < seriesList.length; i++) {
      bufferSeriesList[i] = new SimpleTimeSeriesFromByteBufferAdapter(window, MAX_ENTRIES);
      bufferSeriesList[i].init(mem, buffStartPosition);
      bufferSeriesList[i].setStartBuffered(mem, buffStartPosition, seriesList[i].getStart());
      bufferSeriesList[i].setEndBuffered(mem, buffStartPosition, seriesList[i].getEnd());
      for (int j = 0; j < seriesList[i].size(); j++) {
        bufferSeriesList[i].addDataPointBuffered(mem,
                                                 buffStartPosition,
                                                 seriesList[i].getTimestamps().getLong(j),
                                                 seriesList[i].getDataPoints().getDouble(j));
      }
      seriesToMerge[i] = bufferSeriesList[i].computeSimpleBuffered(mem, buffStartPosition);
    }

    for (SimpleTimeSeries simpleTimeSeries : seriesToMerge) {
      timeSeries.mergeSeriesBuffered(finalMem, buffStartPosition, simpleTimeSeries);
    }

    return timeSeries.computeSimpleBuffered(finalMem, buffStartPosition);
  }
}
