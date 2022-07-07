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

public class TimestampRawDecoderIterator implements LongIterator
{
  private final boolean useInteger;
  private final long minTimestamp;
  private final ByteBuffer timestampsBuffer;
  private final int count;

  private int i = 0;
  private long lastValue = 0;

  public TimestampRawDecoderIterator(boolean useInteger, long minTimestamp, ByteBuffer timestampsBuffer)
  {
    this.useInteger = useInteger;
    this.minTimestamp = minTimestamp;
    this.timestampsBuffer = timestampsBuffer;

    if (timestampsBuffer.hasRemaining()) {
      count = timestampsBuffer.getInt();
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
  public long next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException(StringUtils.format("index %s invalid in size %s", i, count));
    }
    long value = useInteger ? timestampsBuffer.getInt() : timestampsBuffer.getLong();

    if (i == 0) {
      value += minTimestamp;
    } else {
      value += lastValue;
    }

    i++;
    lastValue = value;

    return value;
  }
}
