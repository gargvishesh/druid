/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.currency;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;

import java.nio.ByteBuffer;

public class CurrencySumAggregator implements Aggregator
{
  private final BufferAggregator bufferAggregator;
  private final ByteBuffer buf;

  public CurrencySumAggregator(
      BufferAggregator bufferAggregator,
      ByteBuffer buf
  )
  {
    this.bufferAggregator = bufferAggregator;
    this.buf = buf;
    bufferAggregator.init(buf, 0);
  }

  @Override
  public void aggregate()
  {
    bufferAggregator.aggregate(buf, 0);
  }

  @Override
  public Object get()
  {
    return bufferAggregator.get(buf, 0);
  }

  @Override
  public float getFloat()
  {
    return bufferAggregator.getFloat(buf, 0);
  }

  @Override
  public long getLong()
  {
    return bufferAggregator.getLong(buf, 0);
  }

  @Override
  public double getDouble()
  {
    return bufferAggregator.getDouble(buf, 0);
  }

  @Override
  public void close()
  {
    bufferAggregator.close();
  }
}
