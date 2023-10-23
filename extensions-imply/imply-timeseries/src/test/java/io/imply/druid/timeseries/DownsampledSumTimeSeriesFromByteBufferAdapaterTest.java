/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

import java.nio.ByteBuffer;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public class DownsampledSumTimeSeriesFromByteBufferAdapaterTest extends DownsampledSumTimeSeriesBaseTest
{
  @Override
  public SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window, DurationGranularity durationGranularity)
  {
    WritableMemory mem = WritableMemory.writableWrap(
        ByteBuffer.allocateDirect(DownsampledSumTimeSeriesAggregatorFactory.getTimeseriesBytesSize(MAX_ENTRIES))
    ); // simulate the real deal, keeping the size as tight as possible to check out-of-bounds accesses
    WritableMemory finalMem = WritableMemory.writableWrap(
        ByteBuffer.allocateDirect(DownsampledSumTimeSeriesAggregatorFactory.getTimeseriesBytesSize(MAX_ENTRIES))
    );
    int buffStartPosition = 0;
    DownsampledSumTimeSeriesFromByteBufferAdapter timeSeries = new DownsampledSumTimeSeriesFromByteBufferAdapter(durationGranularity, window, MAX_ENTRIES);
    timeSeries.init(finalMem, buffStartPosition);

    DownsampledSumTimeSeries[] seriesToMerge = new DownsampledSumTimeSeries[seriesList.length];
    DownsampledSumTimeSeriesFromByteBufferAdapter[] bufferSeriesList = new DownsampledSumTimeSeriesFromByteBufferAdapter[seriesList.length];
    for (int i = 0; i < seriesList.length; i++) {
      bufferSeriesList[i] = new DownsampledSumTimeSeriesFromByteBufferAdapter(durationGranularity, window, MAX_ENTRIES);
      bufferSeriesList[i].init(mem, buffStartPosition);
      bufferSeriesList[i].setStartBuffered(mem, buffStartPosition, seriesList[i].getStart());
      bufferSeriesList[i].setEndBuffered(mem, buffStartPosition, seriesList[i].getEnd());
      for (int j = 0; j < seriesList[i].size(); j++) {
        bufferSeriesList[i].addDataPointBuffered(
            mem,
            buffStartPosition,
            seriesList[i].getTimestamps().getLong(j),
            seriesList[i].getDataPoints().getDouble(j)
        );
      }
      seriesToMerge[i] = bufferSeriesList[i].computeDownsampledSumBuffered(mem, buffStartPosition);
    }

    for (DownsampledSumTimeSeries downsampledSumTimeSeries : seriesToMerge) {
      timeSeries.mergeSeriesBuffered(finalMem, buffStartPosition, downsampledSumTimeSeries);
    }

    return timeSeries.computeSimpleBuffered(finalMem, buffStartPosition);
  }
}
