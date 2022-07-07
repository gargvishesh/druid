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

public class DoubleValueRleDecoderIterator implements DoubleIterator
{
  private final ByteBuffer datapointsBuffer;
  private final int runCount;

  // runNumber == -1, currentRunLength = 0, currentRunNumber = 0 | -1 => we'll read the first run and initialize
  // our runNumber = currentRunNumber = 0, and load currentRunLength
  private int runNumber = -1;
  private int currentRunLength = 0;
  private int currentRunNumber = -1;
  private double currentRunValue = -1;

  public DoubleValueRleDecoderIterator(ByteBuffer datapointsBuffer)
  {
    this.datapointsBuffer = datapointsBuffer;
    if (datapointsBuffer.hasRemaining()) {
      runCount = datapointsBuffer.getInt();
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
  public double next()
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

    double result = getNextValue();

    return result;
  }

  private double getNextValue()
  {
    currentRunNumber++;

    if (currentRunNumber >= currentRunLength) {
      readCurrentRun();
      runNumber++;
      currentRunNumber = 0;
    }

    return currentRunValue;
  }

  private void readCurrentRun()
  {
    currentRunLength = datapointsBuffer.getInt();
    currentRunValue = datapointsBuffer.getDouble();
  }
}
