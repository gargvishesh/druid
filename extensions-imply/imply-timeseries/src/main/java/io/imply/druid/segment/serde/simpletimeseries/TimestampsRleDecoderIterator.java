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

public class TimestampsRleDecoderIterator implements LongIterator
{
  private final boolean useInteger;
  private final long minTimestamp;
  private final ByteBuffer timestampsBuffer;
  private final int runCount;

  // runNumber == -1, currentRunLength = 0, currentRunNumber = 0 | -1 => we'll read the first next() call and
  // initialize runNumber = currentRunNumber = 0, and load currentRunLength/currentRunDelta
  private int runNumber = -1;
  private int currentRunLength = 0;
  private int currentRunNumber = -1;
  private long currentRunDelta = -1;
  private long lastValue = 0;
  private boolean first = true;

  public TimestampsRleDecoderIterator(boolean useInteger, long minTimestamp, ByteBuffer timestampsBuffer)
  {
    this.useInteger = useInteger;
    this.minTimestamp = minTimestamp;
    this.timestampsBuffer = timestampsBuffer;

    if (timestampsBuffer.hasRemaining()) {
      runCount = timestampsBuffer.getInt();
    } else {
      runCount = 0;
    }
  }

  @Override
  public boolean hasNext()
  {
    return runCount > 0 && !(runNumber == runCount - 1 && currentRunNumber == currentRunLength - 1);
  }

  @Override
  public long next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException(StringUtils.format(
          "%s %s > [%s %s]",
          runNumber,
          currentRunNumber,
          runCount,
          currentRunLength
      ));
    }

    long result = getNextDelta();

    if (first) {
      result += minTimestamp;
      first = false;
    } else {
      result += lastValue;
    }

    lastValue = result;

    return result;
  }

  private long getNextDelta()
  {
    currentRunNumber++;

    if (currentRunNumber >= currentRunLength) {
      readCurrentRun();
      runNumber++;
    }

    return currentRunDelta;
  }

  private void readCurrentRun()
  {
    currentRunLength = timestampsBuffer.getInt();
    currentRunDelta = useInteger ? timestampsBuffer.getInt() : timestampsBuffer.getLong();
    currentRunNumber = 0;
  }
}
