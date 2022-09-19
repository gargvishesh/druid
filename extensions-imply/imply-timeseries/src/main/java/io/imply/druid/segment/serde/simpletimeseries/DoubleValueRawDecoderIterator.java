/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class DoubleValueRawDecoderIterator implements DoubleIterator
{
  private final ByteBuffer datapointsBuffer;
  private final int count;

  private int i = 0;

  public DoubleValueRawDecoderIterator(ByteBuffer datapointsBuffer)
  {
    this.datapointsBuffer = datapointsBuffer;

    if (datapointsBuffer.hasRemaining()) {
      count = datapointsBuffer.getInt();
    } else {
      count = 0;
    }
  }

  @Override
  public boolean hasNext()
  {
    return i < count;
  }

  @Override
  public double next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException(StringUtils.format("index %s invalid in size %s", i, count));
    }

    double value = datapointsBuffer.getDouble();
    i++;

    return value;
  }
}
