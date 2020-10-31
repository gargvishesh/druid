/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 *  of Imply Data, Inc.
 */

package io.imply.druid.currency;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class CurrencySumBufferAggregator implements BufferAggregator
{
  private static final int NEXT_TIME = 0;
  private static final int CURRENT_CONVERSION = Longs.BYTES;
  private static final int VALUE = Longs.BYTES + Doubles.BYTES;

  private final TreeMap<Long, Double> conversions;
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseDoubleColumnValueSelector metricSelector;

  public CurrencySumBufferAggregator(
      TreeMap<Long, Double> conversions,
      BaseLongColumnValueSelector timeSelector,
      BaseDoubleColumnValueSelector metricSelector
  )
  {
    this.conversions = conversions;
    this.timeSelector = timeSelector;
    this.metricSelector = metricSelector;
  }

  @Override
  public void init(ByteBuffer byteBuffer, int position)
  {
    byteBuffer.putLong(
        position + NEXT_TIME,
        conversions.isEmpty() ? Long.MAX_VALUE : this.conversions.firstEntry().getKey()
    );
    byteBuffer.putDouble(position + CURRENT_CONVERSION, 1);
    byteBuffer.putDouble(position + VALUE, 0);
  }

  @Override
  public void aggregate(ByteBuffer byteBuffer, int position)
  {
    final long t = timeSelector.getLong();

    final long nextTime = byteBuffer.getLong(position + NEXT_TIME);
    final double currentConversion;

    if (t >= nextTime) {
      final Map.Entry<Long, Double> entry = conversions.floorEntry(t);
      if (entry.getKey() < nextTime) {
        throw new ISE("WTF?! Expected next entry to be at or after nextTime[%s]?!", DateTimes.utc(nextTime));
      }
      currentConversion = entry.getValue();
      byteBuffer.putDouble(position + CURRENT_CONVERSION, entry.getValue());
      final Map.Entry<Long, Double> nextEntry = conversions.tailMap(entry.getKey(), false).firstEntry();
      byteBuffer.putLong(position + NEXT_TIME, nextEntry == null ? Long.MAX_VALUE : nextEntry.getKey());
    } else {
      currentConversion = byteBuffer.getDouble(position + CURRENT_CONVERSION);
    }

    byteBuffer.putDouble(
        position + VALUE,
        byteBuffer.getDouble(position + VALUE) + metricSelector.getDouble() * currentConversion
    );
  }

  @Override
  public Object get(ByteBuffer byteBuffer, int position)
  {
    return byteBuffer.getDouble(position + VALUE);
  }

  @Override
  public float getFloat(ByteBuffer byteBuffer, int position)
  {
    return (float) get(byteBuffer, position);
  }

  @Override
  public long getLong(ByteBuffer byteBuffer, int position)
  {
    return (long) get(byteBuffer, position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getDouble(position + VALUE);
  }

  @Override
  public void close()
  {
    // no resources to clean up
  }
}
